# python_backend_services/tests/test_search_orchestrator.py
import pytest
from unittest.mock import MagicMock  # Para criar mocks mais detalhados se necessário

# Tenta importar o SearchOrchestrator e os serviços que ele usa
try:
    from python_backend_services.app.services.search_orchestrator import SearchOrchestrator

    # Não precisamos importar ES, LLM, Glossary aqui diretamente se usamos as fixtures do conftest
    # que fornecem mocks para eles.
    SEARCH_ORCH_AVAILABLE_FOR_TEST = True
except ImportError:
    SEARCH_ORCH_AVAILABLE_FOR_TEST = False


@pytest.mark.skipif(not SEARCH_ORCH_AVAILABLE_FOR_TEST, reason="SearchOrchestrator não pôde ser importado para teste.")
def test_search_orchestrator_init(search_orchestrator_instance: SearchOrchestrator):  # Usa a fixture do conftest
    """Testa a inicialização do SearchOrchestrator."""
    assert search_orchestrator_instance is not None
    assert search_orchestrator_instance.es_service is not None
    assert search_orchestrator_instance.llm_service is not None
    # assert search_orchestrator_instance.glossary_service is not None # Se for passado e esperado


@pytest.mark.skipif(not SEARCH_ORCH_AVAILABLE_FOR_TEST, reason="SearchOrchestrator não pôde ser importado para teste.")
def test_fetch_initial_candidates(search_orchestrator_instance: SearchOrchestrator, mock_es_service):
    """Testa a busca de candidatos iniciais (mockando a resposta do ES)."""
    # A fixture mock_es_service já tem um método search mockado que retorna dados.
    # O search_orchestrator_instance já foi criado com este mock_es_service.

    user_query = "teste de query"
    candidates = search_orchestrator_instance._fetch_initial_candidates_from_es(user_query, size=5)

    # Verifica se o método search do mock_es_service foi chamado corretamente
    assert mock_es_service.search_called_with_params is not None
    assert mock_es_service.search_called_with_params["index"] == search_orchestrator_instance.es_index_name
    assert mock_es_service.search_called_with_params["body"]["query"]["multi_match"]["query"] == user_query

    assert len(candidates) > 0  # O mock retorna 1 candidato
    assert candidates[0]["document_id"] == "mock_doc_1"


@pytest.mark.skipif(not SEARCH_ORCH_AVAILABLE_FOR_TEST, reason="SearchOrchestrator não pôde ser importado para teste.")
def test_search_and_rerank_success(search_orchestrator_instance: SearchOrchestrator, mock_es_service, mock_llm_service):
    """Testa o fluxo completo de search_and_rerank com sucesso (usando mocks)."""
    user_query = "petição de alimentos"

    # mock_es_service já está configurado para retornar um candidato via a fixture.
    # mock_llm_service também está configurado para retornar um resultado de re-ranking.

    result = search_orchestrator_instance.search_and_rerank_documents(user_query)

    assert result is not None
    assert result["chosen_document_id"] == "mock_doc_1"  # LLM mock escolhe o primeiro candidato do ES mock
    assert "contextual_summary_llm" in result
    assert "full_text_content" in result


@pytest.mark.skipif(not SEARCH_ORCH_AVAILABLE_FOR_TEST, reason="SearchOrchestrator não pôde ser importado para teste.")
def test_search_and_rerank_no_es_candidates(search_orchestrator_instance: SearchOrchestrator, mock_es_service):
    """Testa o caso em que o ES não retorna candidatos."""
    mock_es_service.search = MagicMock(return_value={"hits": {"hits": []}})  # Sobrescreve o mock para este teste

    user_query = "query sem resultados"
    result = search_orchestrator_instance.search_and_rerank_documents(user_query)

    assert result is None


@pytest.mark.skipif(not SEARCH_ORCH_AVAILABLE_FOR_TEST, reason="SearchOrchestrator não pôde ser importado para teste.")
def test_search_and_rerank_llm_failure_fallback(search_orchestrator_instance: SearchOrchestrator, mock_es_service,
                                                mock_llm_service):
    """Testa o fallback para o top-1 do ES quando o LLM falha."""
    # mock_es_service retorna um candidato ("mock_doc_1")
    mock_llm_service.rerank_and_summarize = MagicMock(return_value=None)  # Simula falha do LLM

    user_query = "query com falha no llm"
    result = search_orchestrator_instance.search_and_rerank_documents(user_query)

    assert result is not None
    assert result["chosen_document_id"] == "mock_doc_1"  # Deve ser o fallback do ES
    assert "(Fallback ES Top-1)" in result["contextual_summary_llm"]

# Adicione mais testes para:
# - Lógica de feedback (modelo obrigatório, boosting) quando implementada
# - Diferentes respostas do LLM (e.g., ID de documento inválido)
# - Erros nos serviços ES ou LLM e como o orchestrator lida com eles