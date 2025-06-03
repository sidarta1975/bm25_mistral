# python_backend_services/tests/test_llm_service.py
import unittest
from unittest.mock import MagicMock # 'call' não é mais necessário para este teste específico
import logging
import json # Importado para usar json.dumps nos dados de teste

# Importe a classe que você quer testar
from python_backend_services.app.services.llm_service import LLMService

# Opcionalmente, importe GlossaryService para 'spec' no MagicMock.
try:
    from python_backend_services.app.services.glossary_service import GlossaryService
    GLOSSARY_SERVICE_SPEC = GlossaryService
except ImportError:
    GLOSSARY_SERVICE_SPEC = None
    logging.warning("Não foi possível importar GlossaryService para spec em test_llm_service.py. O mock não terá spec.")

# Configuração de logging para ajudar a depurar os testes, se necessário
# Garante que o logging seja configurado apenas uma vez
if not logging.getLogger().hasHandlers():
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - [%(name)s:%(lineno)d] - %(message)s')
logger = logging.getLogger(__name__)


class TestLLMServicePromptBuilding(unittest.TestCase):

    def setUp(self):
        """Este método é chamado antes de cada teste."""
        logger.info("Executando setUp para TestLLMServicePromptBuilding")
        # 1. Simular (mock) o GlossaryService.
        # Embora _build_rerank_summary_prompt não use diretamente o mock_glossary_service
        # para buscar termos (ele espera que os termos já estejam nos dados do candidato),
        # o LLMService é instanciado com ele.
        self.mock_glossary_service = MagicMock(spec=GLOSSARY_SERVICE_SPEC)

        # 2. Instanciar LLMService com o glossary_service mockado.
        # Os URLs e timeouts são para o construtor do LLMService, mas não são usados
        # diretamente por _build_rerank_summary_prompt.
        # Se o LLMService tivesse sido fornecido na primeira mensagem com ollama_generate_url etc.
        # como parâmetros de construtor, eles seriam usados aqui. Assumindo que
        # o construtor de LLMService os pega de 'settings' e o que foi passado
        # na pergunta anterior era para um contexto diferente de inicialização.
        # Para fins deste teste, estamos focando no glossary_service.
        # Se o construtor de LLMService não aceitar esses URLs, remova-os.
        # Vou assumir que o construtor do LLMService só precisa do glossary_service
        # conforme o código de llm_service.py da primeira mensagem.
        self.llm_service_instance = LLMService(
            glossary_service=self.mock_glossary_service
        )
        # Se o construtor real do seu LLMService usa settings para URLs, ele as pegará de lá.
        # Se você passou `ollama_generate_url` etc. no construtor do LLMService na pergunta anterior
        # e ele de fato os aceita, então a instanciação deveria ser como a original:
        # self.llm_service_instance = LLMService(
        #     ollama_generate_url="http://mock-ollama-server/api/generate",
        #     ollama_embeddings_url="http://mock-ollama-server/api/embeddings",
        #     model_name="test-model-mistral",
        #     request_timeout=30,
        #     glossary_service=self.mock_glossary_service
        # )
        # No entanto, o llm_service.py fornecido na primeira mensagem não mostra esses
        # parâmetros no construtor, ele pega de `settings`.
        # A versão atualizada do `llm_service.py` que forneci para você também pega de `settings`.

        logger.info(
            f"LLMService instanciado com mock_glossary_service: {type(self.llm_service_instance.glossary_service)}")

    def test_build_prompt_enriches_with_glossary_names(self): # Nome do teste atualizado
        """
        Testa se _build_rerank_summary_prompt enriquece o prompt
        corretamente com os NOMES dos termos do glossário para os documentos,
        lidos do campo 'document_specific_terms' dos candidatos.
        """
        logger.info("Iniciando test_build_prompt_enriches_with_glossary_names")

        # --- A. Preparar Dados de Teste para o método _build_rerank_summary_prompt ---
        # _build_rerank_summary_prompt espera que os termos do glossário já estejam
        # processados e incluídos nos dados do candidato, especificamente no campo
        # 'document_specific_terms' como uma string JSON de uma lista de nomes de termos.

        test_query = "ação de indenização por dano moral"
        test_candidates = [
            {
                "id": "doc1_id_test",
                "document_id": "doc1_id_test",
                "document_title_original": "Modelo de Petição Inicial", # Usado se document_title_llm não existir
                "summary_original": "Documento 1 sobre como fazer uma petição inicial.", # Usado se summary1_llm não existir
                "first_lines_original": "Excelentíssimo Senhor Doutor Juiz de Direito da Comarca...",
                # Conteúdo completo não é diretamente usado no prompt, mas é parte do dado original
                "content": "Conteúdo completo do documento 1 sobre petição inicial...",
                # ESTE é o campo que _build_rerank_summary_prompt usa para os termos do glossário
                "document_specific_terms": json.dumps(["Petição Inicial", "Requisitos Essenciais"])
            },
            {
                "id": "doc2_id_test",
                "document_id": "doc2_id_test",
                "document_title_original": "Outro Assunto Jurídico",
                "summary_original": "Documento 2 que não contém termos do glossário para este teste.",
                "first_lines_original": "Introdução ao tema abordado no documento.",
                "content": "Conteúdo completo do documento 2 sem termos relevantes.",
                "document_specific_terms": json.dumps([]) # Lista vazia de termos
            },
            {
                "id": "doc3_id_test",
                "document_id": "doc3_id_test",
                "document_title_llm": "Recurso de Dano Moral", # LLMService prioriza este título
                "summary1_llm": "Este é um recurso sobre dano moral.", # LLMService prioriza este sumário
                "first_lines_original": "Egrégio Tribunal...",
                "content": "Conteúdo completo do documento 3 sobre dano moral.",
                "document_specific_terms": None # Simula ausência do campo ou valor None
            }
        ]

        # --- B. Chamar o Método a ser Testado ---
        generated_prompt = self.llm_service_instance._build_rerank_summary_prompt(test_query, test_candidates)

        logger.debug("\n--- PROMPT GERADO PARA ANÁLISE NO TESTE ---\n" + generated_prompt + "\n--- FIM DO PROMPT ---")

        # --- C. Verificar o Prompt Gerado (Asserções) ---
        self.assertIn(f"Consulta do usuário: \"{test_query}\"", generated_prompt)

        # Verificar Documento 1 (doc1_id_test)
        # O prompt deve conter o título e resumo originais, já que _llm não foram fornecidos para este doc
        doc1_section_start = generated_prompt.find("--- Documento Candidato 1 ---")
        doc1_section_end = generated_prompt.find("--- Documento Candidato 2 ---", doc1_section_start)
        doc1_section = generated_prompt[doc1_section_start:doc1_section_end]

        self.assertIn("ID: \"doc1_id_test\"", doc1_section)
        self.assertIn("Título: \"Modelo de Petição Inicial\"", doc1_section)
        self.assertIn("Resumo Disponível: \"Documento 1 sobre como fazer uma petição inicial.\"", doc1_section)
        self.assertIn("Trecho Inicial: \"Excelentíssimo Senhor Doutor Juiz de Direito da Comarca...\"", doc1_section)
        # Verifica a linha com os nomes dos termos do glossário
        self.assertIn("Termos chave identificados neste documento: Petição Inicial, Requisitos Essenciais", doc1_section)

        # Verificar Documento 2 (doc2_id_test)
        doc2_section_start = generated_prompt.find("--- Documento Candidato 2 ---")
        doc2_section_end = generated_prompt.find("--- Documento Candidato 3 ---", doc2_section_start)
        doc2_section = generated_prompt[doc2_section_start:doc2_section_end]

        self.assertIn("ID: \"doc2_id_test\"", doc2_section)
        self.assertIn("Título: \"Outro Assunto Jurídico\"", doc2_section)
        # Como document_specific_terms era uma lista vazia, a linha "Termos chave..." NÃO deve aparecer.
        self.assertNotIn("Termos chave identificados neste documento:", doc2_section,
                         "Seção de termos do glossário apareceu indevidamente para Doc2")

        # Verificar Documento 3 (doc3_id_test)
        doc3_section_start = generated_prompt.find("--- Documento Candidato 3 ---")
        doc3_section_end = generated_prompt.find("--- Fim do Documento Candidato 3 ---", doc3_section_start)