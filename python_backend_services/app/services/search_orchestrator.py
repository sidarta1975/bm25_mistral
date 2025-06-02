# python_backend_services/app/services/search_orchestrator.py
import logging
from typing import List, Dict, Any, Optional
import json

try:
    from python_backend_services.app.core.config import settings
    from python_backend_services.data_ingestion.indexer_service import ElasticsearchService
    from python_backend_services.app.services.llm_service import LLMService
    from python_backend_services.app.services.glossary_service import GlossaryService
    from elasticsearch import exceptions as es_exceptions  # Importar es_exceptions
except ImportError:
    print("search_orchestrator.py: WARNING - Falha nos imports principais. Usando mocks/placeholders se definidos.")
    # Definir Mocks de settings, ElasticsearchService, LLMService, GlossaryService aqui se necessário para testes standalone
    # ou garantir que o ambiente de teste os forneça.
    # Por simplicidade, vamos assumir que em um ambiente de execução normal, os imports funcionam.
    # Em um ambiente de teste com Pytest, as fixtures cuidariam disso.
    # Para execução standalone direta deste arquivo (não recomendado para esta classe),
    # seria necessário um setup de mock mais elaborado aqui.
    settings = type('MockSettings', (object,),
                    {'ELASTICSEARCH_INDEX_NAME': 'mock_index', 'CANDIDATES_FOR_LLM_RERANK': 3})()
    es_exceptions = None  # Apenas para evitar NameError se o import principal falhar

logger = logging.getLogger(__name__)


class SearchOrchestrator:
    def __init__(self,
                 es_service: ElasticsearchService,
                 llm_service: LLMService,
                 glossary_service: Optional[GlossaryService] = None):
        if not es_service:
            raise ValueError("ElasticsearchService é obrigatório para SearchOrchestrator.")
        if not llm_service:
            raise ValueError("LLMService é obrigatório para SearchOrchestrator.")

        self.es_service = es_service
        self.llm_service = llm_service
        self.glossary_service = glossary_service
        self.es_index_name = settings.ELASTICSEARCH_INDEX_NAME
        self.candidates_for_llm = settings.CANDIDATES_FOR_LLM_RERANK

        logger.info("SearchOrchestrator inicializado com sucesso.")
        if self.glossary_service:
            logger.info("SearchOrchestrator: GlossaryService associado.")

    def _fetch_initial_candidates_from_es(self, user_query: str, size: int = 10) -> List[Dict[str, Any]]:
        """
        Busca candidatos iniciais no Elasticsearch usando uma query BM25 (multi_match).
        """
        query_body = {
            "size": size,
            "query": {
                "multi_match": {
                    "query": user_query,
                    "fields": [
                        "document_title_llm^5",
                        "document_title_original^4",  # Populado pelo 'name_text' do TSV
                        "summary1_llm^4",
                        "summary2_llm^3",
                        "full_text_content^2",  # Conteúdo completo do arquivo .txt
                        "summary_original^2",  # Populado pelo 'summary' do TSV
                        "legal_action_original^1.5",
                        "document_specific_terms^3"  # Termos do glossário encontrados no documento
                    ],
                    "type": "best_fields",
                    "fuzziness": "AUTO"
                }
            },
        }

        try:
            logger.debug(
                f"Executando query ES no índice '{self.es_index_name}': {json.dumps(query_body, ensure_ascii=False)}")
            response = self.es_service.es_client.search(
                index=self.es_index_name,
                body=query_body
            )

            candidates = []
            if response and 'hits' in response and 'hits' in response['hits']:
                for hit in response['hits']['hits']:
                    candidate_doc = hit.get('_source', {})  # Usar .get para segurança
                    candidate_doc['id'] = hit['_id']
                    candidate_doc['es_score'] = hit.get('_score')
                    candidates.append(candidate_doc)
            else:
                logger.warning(
                    f"Resposta inesperada ou vazia do Elasticsearch para a query: '{user_query}'. Resposta: {response}")

            logger.info(f"Elasticsearch retornou {len(candidates)} candidatos iniciais para a query: '{user_query}'")
            return candidates
        except es_exceptions.NotFoundError:
            logger.warning(f"Índice '{self.es_index_name}' não encontrado no Elasticsearch para query '{user_query}'.")
            return []
        except es_exceptions.ElasticsearchException as e:
            logger.error(f"Erro ao buscar candidatos do Elasticsearch para query '{user_query}': {e}", exc_info=True)
            return []
        except Exception as e_gen:
            logger.error(f"Erro geral em _fetch_initial_candidates_from_es para query '{user_query}': {e_gen}",
                         exc_info=True)
            return []

    def search_and_rerank_documents(self, user_query: str) -> Optional[Dict[str, Any]]:
        logger.info(f"Orquestrando busca para query: '{user_query}'")

        initial_candidates = self._fetch_initial_candidates_from_es(user_query,
                                                                    size=self.candidates_for_llm * 2)
        if not initial_candidates:
            logger.warning(f"Nenhum candidato inicial encontrado no Elasticsearch para: '{user_query}'")
            return None

        initial_candidate_ids = [str(doc.get("id")) for doc in initial_candidates if
                                 doc.get("id") is not None]  # Garantir que IDs sejam strings
        logger.debug(f"IDs dos candidatos iniciais do ES (para LLM): {initial_candidate_ids[:self.candidates_for_llm]}")

        candidates_for_llm_rerank = initial_candidates[:self.candidates_for_llm]

        if not candidates_for_llm_rerank:  # Checagem adicional
            logger.warning(f"A lista de candidatos para LLM ficou vazia após o slice para: '{user_query}'")
            return None

        logger.info(
            f"Enviando {len(candidates_for_llm_rerank)} candidatos para LLM para re-ranking. IDs: {[doc.get('id') for doc in candidates_for_llm_rerank]}")

        llm_result = self.llm_service.rerank_and_summarize(user_query, candidates_for_llm_rerank)
        logger.debug(f"Resultado do LLM (rerank_and_summarize): {llm_result}")

        if llm_result and llm_result.get("chosen_document_id") and not llm_result.get("error"):
            chosen_doc_id_from_llm = str(llm_result["chosen_document_id"])  # Garantir que seja string
            contextual_summary = llm_result["contextual_summary"]
            reasoning = llm_result.get("reasoning", "Não fornecida.")

            logger.info(
                f"LLM escolheu o documento ID: '{chosen_doc_id_from_llm}'. Tentando encontrar nos candidatos iniciais.")

            # Tenta encontrar o documento na lista original de candidatos do ES
            chosen_document_data = next(
                (doc for doc in initial_candidates if str(doc.get("id")) == chosen_doc_id_from_llm), None)

            if chosen_document_data:
                logger.info(
                    f"Documento ID '{chosen_doc_id_from_llm}' encontrado nos candidatos. Título: '{chosen_document_data.get('document_title_original')}'")
                api_response = {
                    "chosen_document_id": chosen_doc_id_from_llm,  # Usar o ID retornado pelo LLM
                    "document_title": chosen_document_data.get("document_title_llm") or chosen_document_data.get(
                        "document_title_original") or chosen_document_data.get("name_text"),
                    # Adicionado name_text como fallback
                    "contextual_summary_llm": contextual_summary,
                    "summary1_llm_original": chosen_document_data.get("summary1_llm"),
                    "summary2_llm_original": chosen_document_data.get("summary2_llm"),
                    "reasoning_llm": reasoning,
                    "full_text_content": chosen_document_data.get("full_text_content"),
                    "file_name": chosen_document_data.get("file_name"),
                    "es_score_original_chosen_doc": chosen_document_data.get("es_score")
                }
                return api_response
            else:
                logger.error(
                    f"Documento escolhido pelo LLM (ID: '{chosen_doc_id_from_llm}') NÃO ENCONTRADO na lista de initial_candidates (IDs: {initial_candidate_ids}).")
                return {
                    "error": f"Documento escolhido pelo LLM ({chosen_doc_id_from_llm}) não encontrado na lista de candidatos iniciais.",
                    "results": []}

        else:
            logger.warning(
                f"LLM não conseguiu re-rankear ou falhou para a query '{user_query}'. Resultado do LLM: {llm_result}")
            if initial_candidates:  # Fallback se o LLM falhou mas o ES retornou algo
                logger.info("Aplicando fallback: Retornando o candidato top 1 do Elasticsearch.")
                top_candidate_es = initial_candidates[0]
                simple_summary_for_fallback = top_candidate_es.get("summary1_llm") or \
                                              top_candidate_es.get("summary2_llm") or \
                                              top_candidate_es.get("summary_original") or \
                                              top_candidate_es.get("summary") or \
                                              "Resumo não disponível."  # Adicionado fallback para summary
                api_response_fallback = {
                    "chosen_document_id": str(top_candidate_es.get("id")),  # Garantir que seja string
                    "document_title": top_candidate_es.get("document_title_llm") or top_candidate_es.get(
                        "document_title_original") or top_candidate_es.get("name_text"),
                    "contextual_summary_llm": f"(Fallback ES Top-1) {simple_summary_for_fallback}",
                    "summary1_llm_original": top_candidate_es.get("summary1_llm"),
                    "summary2_llm_original": top_candidate_es.get("summary2_llm"),
                    "reasoning_llm": "Fallback para o resultado mais relevante do Elasticsearch devido à falha do re-ranking LLM.",
                    "full_text_content": top_candidate_es.get("full_text_content"),
                    "file_name": top_candidate_es.get("file_name"),
                    "es_score_original_chosen_doc": top_candidate_es.get("es_score")
                }
                return api_response_fallback
            else:
                return None
