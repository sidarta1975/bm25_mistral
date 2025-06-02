# python_backend_services/app/services/llm_service.py
import httpx
import logging
from typing import List, Dict, Any, Optional, Union
import json
import os  # Adicionado para os.path.exists e os.makedirs no __main__
import time  # Adicionado para time.sleep no __main__ (se necessário para testes)

# Tenta importar o GlossaryService real e settings.
_RealGlossaryService = None
_MockGlossaryService = None  # Usado apenas para fallback no __main__ deste arquivo

try:
    from python_backend_services.app.core.config import settings
    from python_backend_services.app.services.glossary_service import GlossaryService as RealGlossaryServiceImported

    _RealGlossaryService = RealGlossaryServiceImported
    # Configuração de logging aqui, usando settings se importado com sucesso
    if not logging.getLogger().hasHandlers():
        logging.basicConfig(level=settings.LOG_LEVEL.upper(),
                            format='%(asctime)s - %(levelname)s - [%(name)s:%(lineno)d] - %(message)s')
except ImportError:
    # Fallback para execução standalone ou testes unitários deste arquivo específico
    print("llm_service.py: WARNING - Could not import 'settings' or 'GlossaryService' from project. Using Mocks.")


    class MockSettingsLLM:
        OLLAMA_API_URL = "http://localhost:11434/api/generate"
        OLLAMA_EMBEDDINGS_API_URL = "http://localhost:11434/api/embeddings"
        OLLAMA_MODEL_NAME = "mistral:7b"
        OLLAMA_REQUEST_TIMEOUT = 180
        GLOSSARY_FILE_PATH = "mock_glossary_for_llm_service.tsv"  # Caminho para um mock
        LOG_LEVEL = "DEBUG"


    settings = MockSettingsLLM()


    class MockGlossaryServiceInternal:
        def __init__(self, glossary_tsv_path: str):
            self.path = glossary_tsv_path
            self.mock_terms = {"dano moral": {"definicao": "Lesão a bem jurídico extrapatrimonial.",
                                              "assunto1": "Responsabilidade Civil"}}
            # Usar o logger global que será configurado abaixo
            logging.info(f"MockGlossaryServiceInternal inicializado com mock_path: {self.path}")

        def find_terms_in_text(self, text: str, include_details: bool = True) -> Union[List[Dict[str, Any]], List[str]]:
            found = []
            text_lower = text.lower()
            for term, details in self.mock_terms.items():
                if term in text_lower:
                    if include_details:
                        found.append({"termo_original": term, **details})
                    else:
                        found.append(term)
            return found


    _MockGlossaryService = MockGlossaryServiceInternal

    # Configuração de logging de fallback
    if not logging.getLogger().hasHandlers():
        logging.basicConfig(level=settings.LOG_LEVEL.upper(),
                            format='%(asctime)s - %(levelname)s - [%(name)s:%(lineno)d] - %(message)s')

logger = logging.getLogger(__name__)


class LLMService:
    def __init__(self, glossary_service: Optional[Union[RealGlossaryServiceImported, _MockGlossaryService]] = None):
        self.api_url = settings.OLLAMA_API_URL
        self.model_name = settings.OLLAMA_MODEL_NAME
        self.timeout = settings.OLLAMA_REQUEST_TIMEOUT
        self.glossary_service = glossary_service

        # Para esta fase, vamos criar o cliente HTTPX em cada chamada síncrona usando 'with'.
        # Se for necessário reutilizar o cliente (e.g., para manter conexões vivas ou em um contexto async),
        # ele pode ser instanciado aqui e um método close_client() pode ser adicionado.
        # No entanto, para chamadas síncronas e esporádicas, 'with httpx.Client(...)' é seguro.
        logger.info(f"LLMService inicializado. API: {self.api_url}, Modelo: {self.model_name}")
        if self.glossary_service:
            logger.info("GlossaryService fornecido e associado ao LLMService.")
        else:
            logger.warning(
                "LLMService inicializado SEM GlossaryService. Funcionalidades dependentes podem ser limitadas.")

    def _call_ollama_api_sync(self, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Método síncrono interno para chamar a API Ollama."""
        payload_internal = payload.copy()
        payload_internal["stream"] = False  # Garantir que stream seja False para resposta única

        try:
            with httpx.Client(timeout=self.timeout) as client:
                logger.debug(
                    f"Enviando payload para Ollama ({self.api_url}): {json.dumps(payload_internal, indent=2, ensure_ascii=False)}")
                response = client.post(self.api_url, json=payload_internal)
                response.raise_for_status()  # Levanta exceção para respostas 4xx/5xx
                response_json = response.json()
                logger.debug(
                    f"Resposta JSON recebida do Ollama: {json.dumps(response_json, indent=2, ensure_ascii=False)}")
                return response_json
        except httpx.HTTPStatusError as e:
            error_content = "N/A"
            try:
                error_content = e.response.text
            except Exception:
                pass
            logger.error(
                f"Erro HTTP {e.response.status_code} ao chamar Ollama. URL: {e.request.url}. Resposta: {error_content}",
                exc_info=True)
        except httpx.RequestError as e:
            logger.error(f"Erro de requisição ao chamar Ollama (URL: {e.request.url if e.request else 'N/A'}): {e}",
                         exc_info=True)
        except json.JSONDecodeError as e:
            # Para JSONDecodeError, é útil ver o texto que falhou no parse
            logger.error(
                f"Erro ao decodificar JSON da resposta do Ollama. Texto da resposta: '{e.doc if hasattr(e, 'doc') else 'N/A'}'. Erro: {e}",
                exc_info=True)
        except Exception as e_gen:  # Captura qualquer outra exceção
            logger.error(f"Erro inesperado em _call_ollama_api_sync: {e_gen}", exc_info=True)
        return None

    def generate_text(self, prompt: str, context_ollama: Optional[List[int]] = None, expect_json: bool = False) -> \
    Optional[Union[str, Dict[str, Any]]]:
        """
        Gera texto usando o LLM com um prompt específico.
        Pode opcionalmente tentar parsear a resposta como JSON.
        'context_ollama' é o array de inteiros retornado por Ollama para manter estado entre chamadas.
        """
        payload = {
            "model": self.model_name,
            "prompt": prompt,
            "stream": False,
        }
        if context_ollama:
            payload["context"] = context_ollama
        if expect_json:
            payload["format"] = "json"
            logger.info("Solicitando formato JSON ao Ollama para o prompt.")

        response_data = self._call_ollama_api_sync(payload)

        if not response_data:
            logger.warning(f"Resposta nula ou falha na chamada à API Ollama para o prompt: '{prompt[:100]}...'")
            return None

        # O campo 'context' (estado da conversa) é retornado por Ollama mesmo com stream=False
        # new_context_ollama = response_data.get("context") # Pode ser usado para chamadas subsequentes

        if expect_json:
            # Se format=json foi solicitado, Ollama pode retornar o JSON diretamente no campo "response" (como string)
            # ou, para alguns modelos, a resposta inteira pode ser o JSON.
            if "response" in response_data:
                generated_content_str = response_data["response"].strip()
                try:
                    parsed_json = json.loads(generated_content_str)
                    logger.info("Resposta LLM (string em 'response') parseada como JSON com sucesso.")
                    return parsed_json  # Retorna o JSON parseado e o novo contexto
                except json.JSONDecodeError as e:
                    logger.error(
                        f"Falha ao parsear campo 'response' como JSON, embora solicitado. Erro: {e}. 'response' crua: '{generated_content_str}'",
                        exc_info=False)  # exc_info=False para não poluir muito
                    # Retornar um dicionário de erro estruturado
                    return {"error": "Failed to parse LLM response field as JSON",
                            "raw_response_field": generated_content_str}
            else:
                # Se não há 'response' mas esperamos JSON, talvez a resposta inteira seja o JSON
                # Isso é menos comum para format=json com /api/generate, mas verificamos
                logger.warning(f"Campo 'response' não encontrado na resposta do Ollama para prompt JSON. "
                               f"Resposta completa: {response_data}. Tentando tratar a resposta completa como JSON.")
                # Não há muito o que fazer aqui se a estrutura esperada não vier.
                # A melhor abordagem é confiar que, com format=json, o conteúdo estará em 'response'.
                # Se a resposta inteira fosse o JSON, a lógica de 'response_data and "response" in response_data' falharia
                # e cairia no retorno de erro abaixo. Vamos simplificar.
                return {"error": "LLM response JSON structure unexpected", "raw_response_data": response_data}
        else:  # Não esperamos JSON, apenas texto
            if "response" in response_data:
                return response_data["response"].strip()
            else:
                logger.warning(f"Campo 'response' não encontrado na resposta de texto do LLM. Dados: {response_data}")
                return None  # Ou uma string de erro

    def _build_rerank_summary_prompt(self, query: str, candidates: List[Dict[str, Any]]) -> str:
        """
        Constrói o prompt para o LLM realizar re-ranking e sumarização contextual.
        """
        prompt_header = (
            f"Você é um assistente jurídico especializado em analisar e classificar documentos legais com base em uma consulta.\n"
            f"Consulta do usuário: \"{query}\"\n\n"
            f"Analise os seguintes documentos candidatos e suas informações. "
            f"Escolha o documento MAIS RELEVANTE para a consulta e forneça um resumo conciso e contextualizado "
            f"desse documento escolhido, explicando por que ele é o mais relevante para a consulta original.\n\n"
            f"Responda OBRIGATORIAMENTE no seguinte formato JSON. Não inclua nenhum texto antes ou depois do JSON:\n"  # Instrução mais forte
            f"{{\n"
            f'  "chosen_document_id": "id_do_documento_escolhido_EXATO_como_fornecido",\n'  # Enfatiza ID exato
            f'  "contextual_summary": "Seu resumo conciso e contextualizado do documento escolhido, explicando a relevância para a consulta.",\n'
            f'  "reasoning": "Breve justificativa da escolha baseada na consulta e no conteúdo do documento."\n'  # Justificativa mais clara
            f"}}\n\n"
            f"Candidatos (analise todos antes de decidir):\n"  # Instrução para analisar todos
        )

        prompt_candidates_parts = []
        for i, candidate_doc in enumerate(candidates):
            doc_id = candidate_doc.get("id", f"candidato_num_{i + 1}")  # ID mais robusto
            # Prioriza títulos e resumos gerados pelo LLM se disponíveis, senão os originais
            title = candidate_doc.get("document_title_llm") or \
                    candidate_doc.get("document_title_original") or \
                    "Título não disponível"
            summary = candidate_doc.get("summary1_llm") or \
                      candidate_doc.get("summary_original") or \
                      "Resumo não disponível"
            first_lines = candidate_doc.get("first_lines_original", "Primeiras linhas não disponíveis")

            glossary_terms_info_str = ""
            if self.glossary_service:
                doc_specific_terms_json = candidate_doc.get("document_specific_terms")  # Espera-se uma string JSON
                doc_terms_list = []
                if doc_specific_terms_json and isinstance(doc_specific_terms_json, str):
                    try:
                        loaded_terms = json.loads(doc_specific_terms_json)
                        if isinstance(loaded_terms, list):
                            doc_terms_list = loaded_terms
                    except json.JSONDecodeError:
                        logger.debug(
                            f"Falha ao parsear document_specific_terms para doc ID {doc_id}: {doc_specific_terms_json}")

                if doc_terms_list:
                    glossary_terms_info_str = f"\n    Termos chave identificados neste documento: {', '.join(doc_terms_list)}"

            candidate_str = (
                f"--- Documento Candidato {i + 1} ---\n"
                f"ID: \"{doc_id}\"\n"  # Garante que ID esteja entre aspas para o LLM
                f"Título: \"{title}\"\n"
                f"Resumo Disponível: \"{summary}\"\n"
                f"Trecho Inicial: \"{first_lines}\""
                f"{glossary_terms_info_str}\n"
                f"--- Fim do Documento Candidato {i + 1} ---\n"
            )
            prompt_candidates_parts.append(candidate_str)

        return prompt_header + "\n".join(prompt_candidates_parts)

    def rerank_and_summarize(self, query: str, candidates: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """
        Envia o prompt de re-ranking para o LLM e espera uma resposta JSON.
        Retorna o dicionário JSON parseado ou um dicionário de erro.
        """
        if not candidates:
            logger.warning("Nenhum candidato fornecido para re-ranking.")
            return {"error": "No candidates provided", "chosen_document_id": None, "contextual_summary": None,
                    "reasoning": None}

        prompt_for_rerank = self._build_rerank_summary_prompt(query, candidates)

        llm_response = self.generate_text(prompt_for_rerank, expect_json=True)

        if isinstance(llm_response, dict):
            if "error" in llm_response:  # Se generate_text retornou um erro estruturado
                logger.error(f"generate_text reportou um erro para re-ranking: {llm_response}")
                return llm_response  # Repassa o erro
            elif "chosen_document_id" in llm_response and "contextual_summary" in llm_response:
                # Validação adicional: chosen_document_id deve estar entre os IDs dos candidatos
                candidate_ids = {c.get("id") for c in candidates}
                chosen_id = llm_response.get("chosen_document_id")
                if chosen_id not in candidate_ids:
                    logger.warning(
                        f"LLM escolheu um document_id ('{chosen_id}') que não estava entre os candidatos ({candidate_ids}).")
                    # Pode-se tratar isso como um erro ou tentar encontrar o mais similar. Por ora, logamos.
                logger.info(
                    f"LLM re-ranking: escolheu ID '{chosen_id}'. Sumário: '{llm_response.get('contextual_summary', '')[:50]}...'")
                return llm_response
            else:
                logger.error(
                    f"Resposta JSON do LLM para re-ranking não continha os campos esperados (chosen_document_id, contextual_summary). Recebido: {llm_response}")
                return {"error": "LLM response JSON malformed for rerank", "raw_response_data": llm_response,
                        "chosen_document_id": None, "contextual_summary": None, "reasoning": None}

        logger.error(
            f"Falha ao obter ou parsear resposta JSON do LLM para re-ranking. Resposta de generate_text: {llm_response}")
        return {"error": "Failed to get valid JSON response from LLM for rerank",
                "raw_response_content": str(llm_response), "chosen_document_id": None, "contextual_summary": None,
                "reasoning": None}


# Bloco de teste standalone (opcional, mas útil)
if __name__ == '__main__':
    # Logging já configurado no topo do arquivo (no try-except do import)
    logger.info("--- Testando LLMService Standalone ---")

    # Gerenciamento do GlossaryService para o teste
    gloss_service_for_llm_test = None
    temp_mock_glossary_file_created = False
    mock_glossary_path_for_test_main = ""  # Renomeado para evitar conflito com var no fallback

    if _RealGlossaryService:
        try:
            actual_glossary_path_from_settings_main = settings.GLOSSARY_FILE_PATH
            if not os.path.exists(actual_glossary_path_from_settings_main):
                logger.warning(
                    f"Arquivo de glossário em '{actual_glossary_path_from_settings_main}' (de settings) não encontrado. "
                    f"Criando mock temporário para teste do LLMService.")
                mock_glossary_path_for_test_main = os.path.join(os.path.dirname(__file__), "temp_llm_test_glossary.tsv")
                os.makedirs(os.path.dirname(mock_glossary_path_for_test_main), exist_ok=True)
                with open(mock_glossary_path_for_test_main, 'w', encoding='utf-8') as f_mock_gloss:
                    f_mock_gloss.write(
                        "ID_Termo\ttermo_juridico\tdefinicao\tsinonimos\ttermos_relacionados\tcontexto_uso\tfonte_confiavel\tassunto1\tassunto2\tlegal_domain 1\tsub_area_of_law1\n")
                    f_mock_gloss.write(
                        "1\tdano moral\tLesão a bem extrapatrimonial.\tSofrimento\tResponsabilidade Civil\tEm casos de ofensa\tSTJ\tCivil\tIndenização\tDireito Civil\tObrigações\n")
                    f_mock_gloss.write(
                        "2\tusucapião\tModo de aquisição de propriedade.\t\tDireitos Reais\tPosse prolongada\tCódigo Civil\tCivil\tImobiliário\tDireito Civil\tPropriedade\n")
                    f_mock_gloss.write(
                        "3\talimentos\tPensão para subsistência.\tPensão alimentícia\tDireito de Família\tNecessidade\tLei de Alimentos\tFamília\tMenores\tDireito de Família\tAlimentos\n")
                temp_mock_glossary_file_created = True
                gloss_service_for_llm_test = _RealGlossaryService(glossary_tsv_path=mock_glossary_path_for_test_main)
            else:
                gloss_service_for_llm_test = _RealGlossaryService(
                    glossary_tsv_path=actual_glossary_path_from_settings_main)

            if gloss_service_for_llm_test and not gloss_service_for_llm_test.glossary_data:
                logger.warning("RealGlossaryService instanciado para teste, mas não carregou termos.")
        except Exception as e_real_gs:
            logger.error(f"Erro ao instanciar RealGlossaryService para teste: {e_real_gs}", exc_info=True)
            gloss_service_for_llm_test = None

    if not gloss_service_for_llm_test and _MockGlossaryService:
        gloss_service_for_llm_test = _MockGlossaryService(glossary_tsv_path=settings.GLOSSARY_FILE_PATH)
        logger.info("Usando MockGlossaryServiceInternal para o teste do LLMService.")

    if not gloss_service_for_llm_test:
        logger.error("LLMService Test: Nenhum GlossaryService pôde ser configurado. Testes de glossário podem falhar.")

    llm_service_instance = LLMService(glossary_service=gloss_service_for_llm_test)

    # Teste 1: Geração de texto simples
    logger.info("\n--- Testando generate_text (simples) ---")
    simple_prompt = "Explique o que é usucapião em uma frase concisa."
    simple_response = llm_service_instance.generate_text(simple_prompt)
    if simple_response and not (isinstance(simple_response, dict) and "error" in simple_response):
        print(f"Prompt: {simple_prompt}")
        print(f"Resposta LLM (simples): {simple_response}")
    else:
        print(f"Falha ao gerar texto simples. Prompt: {simple_prompt}. Resposta/Erro: {simple_response}")

    # Teste 2: Geração de texto esperando JSON
    logger.info("\n--- Testando generate_text (esperando JSON) ---")
    json_prompt_text = (
        "Liste três características do contrato de trabalho em formato JSON, "
        "com as chaves 'caracteristica_1', 'caracteristica_2', e 'caracteristica_3'."
    )
    json_response = llm_service_instance.generate_text(json_prompt_text, expect_json=True)
    if json_response and not (isinstance(json_response, dict) and "error" in json_response.get("error",
                                                                                               {})):  # Checa erro dentro do dict de erro
        print(f"Prompt: {json_prompt_text}")
        print(f"Resposta LLM (JSON):")
        if isinstance(json_response, dict):
            print(json.dumps(json_response, indent=2, ensure_ascii=False))
        else:
            print(json_response)
    else:
        print(f"Falha ao gerar texto JSON. Prompt: {json_prompt_text}. Resposta/Erro: {json_response}")

    # Teste 3: Re-ranking e sumarização
    logger.info("\n--- Testando rerank_and_summarize ---")
    sample_query_for_rerank_main = "Qual o prazo para contestar ação de alimentos com pedido de dano moral?"
    sample_candidates_for_rerank_main = [
        {"id": "doc1_test", "document_title_original": "Contestação Ação Alimentos",
         "summary1_llm": "Contestação de alimentos e dano moral.",
         "first_lines_original": "Excelentíssimo...",
         "document_specific_terms": json.dumps(["alimentos", "dano moral", "contestação"])},
        {"id": "doc2_test", "document_title_original": "Recurso Dano Moral Contratos",
         "summary_original": "Dano moral em contratos.",
         "first_lines_original": "Egrégio Tribunal...",
         "document_specific_terms": json.dumps(["dano moral", "contratos"])},
        {"id": "doc3_test", "document_title_original": "Usucapião Extraordinário Petição",
         "summary1_llm": "Usucapião de imóvel.",
         "first_lines_original": "Ao douto juízo...",
         "document_specific_terms": json.dumps(["usucapião"])},
    ]

    rerank_result = llm_service_instance.rerank_and_summarize(sample_query_for_rerank_main,
                                                              sample_candidates_for_rerank_main)
    if rerank_result and not rerank_result.get("error"):
        print(f"Resultado do Re-ranking e Sumarização:")
        print(json.dumps(rerank_result, indent=2, ensure_ascii=False))
    else:
        print(f"Falha no re-ranking e sumarização. Resultado/Erro: {rerank_result}")

    if temp_mock_glossary_file_created and os.path.exists(mock_glossary_path_for_test_main):
        try:
            os.remove(mock_glossary_path_for_test_main)
            logger.info(f"Arquivo mock de glossário temporário '{mock_glossary_path_for_test_main}' removido.")
        except OSError as e_rm:
            logger.warning(
                f"Não foi possível remover o arquivo mock de glossário temporário '{mock_glossary_path_for_test_main}': {e_rm}")

    logger.info("--- Testes Standalone do LLMService Concluídos ---")