# python_backend_services/app/services/llm_service.py
# No changes from v6.
import requests
import json
import logging
from typing import List, Dict, Optional, Any

try:
    from python_backend_services.app.core.config import settings
except ImportError:
    class FallbackSettingsLLM:
        OLLAMA_API_URL = "http://localhost:11434/api/generate"
        OLLAMA_MODEL_NAME = "mistral:7b"
        OLLAMA_REQUEST_TIMEOUT = 120


    settings = FallbackSettingsLLM()  # type: ignore
    logging.warning("LLM Service: Failed to import app settings. Using fallback settings.")

logger = logging.getLogger(__name__)


class MistralReranker:
    def __init__(self, ollama_api_url: Optional[str] = None,
                 model_name: Optional[str] = None,
                 timeout: Optional[int] = None):
        self.ollama_api_url = ollama_api_url if ollama_api_url is not None else settings.OLLAMA_API_URL
        self.model_name = model_name if model_name is not None else settings.OLLAMA_MODEL_NAME
        self.timeout = timeout if timeout is not None else settings.OLLAMA_REQUEST_TIMEOUT
        logger.info(f"MistralReranker initialized for model '{self.model_name}' at '{self.ollama_api_url}'")

    def rerank_candidates(self, query: str, candidate_documents: List[Dict[str, Any]]) -> Optional[str]:
        if not candidate_documents:
            logger.warning("No candidate documents provided for reranking.")
            return None

        prompt_parts = [f"User Query: \"{query}\"\n"]
        prompt_parts.append(
            "Based on the user query, which of the following documents is the most relevant? Provide only the document ID of your choice from the list below. Do not add any other text or explanation.\n")

        for i, doc in enumerate(candidate_documents):
            content_preview = doc.get('content', '')[:1500]
            prompt_parts.append(f"\n--- Document Start ---")
            prompt_parts.append(f"Document ID: {doc.get('id')}")
            prompt_parts.append(f"Content Preview: {content_preview}")
            prompt_parts.append(f"--- Document End ---\n")

        full_prompt = "\n".join(prompt_parts)
        logger.debug(f"Prompt for Ollama reranking (first 500 chars): {full_prompt[:500]}...")

        payload = {
            "model": self.model_name,
            "prompt": full_prompt,
            "stream": False,
            "options": {
                "temperature": 0.1
            }
        }
        response_obj = None
        try:
            response_obj = requests.post(self.ollama_api_url, json=payload, timeout=self.timeout)
            response_obj.raise_for_status()
            response_json = response_obj.json()

            llm_output_text = response_json.get("response", "").strip()
            logger.info(f"Ollama raw response for reranking: '{llm_output_text}'")

            for doc_candidate in candidate_documents:
                doc_id = str(doc_candidate.get('id'))
                if doc_id == llm_output_text:
                    logger.info(f"LLM reranking selected document ID: {doc_id}")
                    return doc_id
                if doc_id in llm_output_text and len(llm_output_text) < (len(doc_id) + 15):
                    logger.warning(
                        f"LLM response was not an exact ID match, but found ID '{doc_id}' in a short response: '{llm_output_text}'. Selecting it.")
                    return doc_id

            logger.warning(
                f"Could not reliably parse a document ID from LLM response: '{llm_output_text}'. Expected one of {[str(d.get('id')) for d in candidate_documents]}.")
            return None

        except requests.exceptions.Timeout:
            logger.error(f"Timeout calling Ollama API at {self.ollama_api_url}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Error calling Ollama API: {e}")
            return None
        except json.JSONDecodeError:
            logger.error(
                f"Error decoding JSON response from Ollama: {response_obj.text if response_obj else 'No response object'}")
            return None


