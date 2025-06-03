# python_backend_services/tests/test_search_api.py
import json
import pytest
from flask.testing import FlaskClient  # Para type hinting

# Tenta importar o SearchOrchestrator para mock.
try:
    from python_backend_services.app.services.search_orchestrator import SearchOrchestrator

    SEARCH_ORCH_AVAILABLE = True
except ImportError:
    SEARCH_ORCH_AVAILABLE = False


def test_health_check(client: FlaskClient):
    """Testa o endpoint de health check."""
    response = client.get('/health')  # O endpoint /health está no main.py, não no search_bp
    assert response.status_code == 200
    json_data = response.get_json()
    assert json_data['status'] == 'healthy'  # Ou 'unhealthy' dependendo do estado dos mocks/serviços reais


@pytest.mark.skipif(not SEARCH_ORCH_AVAILABLE, reason="SearchOrchestrator não pôde ser importado.")
def test_search_documents_success(client: FlaskClient, mocker):
    """Testa o endpoint /api/v1/search com uma query válida, esperando sucesso."""

    # Mock para o método search_and_rerank_documents do SearchOrchestrator
    # que está armazenado em current_app.extensions['search_orchestrator']
    mock_search_result = {
        "chosen_document_id": "doc_test_123",
        "document_title": "Petição Teste Mock",
        "contextual_summary_llm": "Este é um resumo mock gerado pelo LLM para a petição de teste.",
        "full_text_content": "Conteúdo completo da petição de teste mock...",
        "file_name": "teste_mock.txt"
    }

    # Acessa o orchestrator da app e mocka seu método
    # Isso requer que a fixture 'app' (e portanto 'client') já tenha configurado o orchestrator
    # na app.extensions['search_orchestrator']
    # Se o orchestrator real não for mockado no conftest, podemos mockar seu método aqui.

    # Para mockar um método de uma instância já criada e colocada em app.extensions
    # é um pouco mais complicado que mockar uma classe antes da instanciação.
    # Uma abordagem é mockar a classe SearchOrchestrator ANTES que 'create_app' seja chamado,
    # ou passar um orchestrator mockado para 'create_app' via test_config.

    # Alternativa: Mock o método diretamente na instância se você puder acessá-la
    # Esta abordagem é mais simples se você tem a instância.
    # No 'app' fixture do conftest, o search_orchestrator é colocado em app.extensions.
    # Então, podemos acessá-lo via current_app (mas current_app só funciona dentro de um contexto de request).
    # É melhor usar o 'mocker' do pytest-mock para mockar o método na classe ANTES que ele seja chamado.

    # Mock para SearchOrchestrator.search_and_rerank_documents
    # Este mock será usado quando o endpoint chamar o método.
    mocker.patch(
        'python_backend_services.app.services.search_orchestrator.SearchOrchestrator.search_and_rerank_documents',
        return_value=mock_search_result
    )

    query_data = {"query": "petição de alimentos"}
    response = client.post('/api/v1/search', json=query_data)

    assert response.status_code == 200
    json_data = response.get_json()
    assert json_data["chosen_document_id"] == "doc_test_123"
    assert "contextual_summary_llm" in json_data
    assert "full_text_content" in json_data


@pytest.mark.skipif(not SEARCH_ORCH_AVAILABLE, reason="SearchOrchestrator não pôde ser importado.")
def test_search_documents_no_query(client: FlaskClient):
    """Testa o endpoint /api/v1/search sem fornecer uma query."""
    response = client.post('/api/v1/search', json={})
    assert response.status_code == 400
    json_data = response.get_json()
    assert "error" in json_data
    assert "query' é obrigatória" in json_data["error"]


@pytest.mark.skipif(not SEARCH_ORCH_AVAILABLE, reason="SearchOrchestrator não pôde ser importado.")
def test_search_documents_empty_query(client: FlaskClient):
    """Testa o endpoint /api/v1/search com uma query vazia."""
    response = client.post('/api/v1/search', json={"query": "   "})
    assert response.status_code == 400
    json_data = response.get_json()
    assert "error" in json_data
    assert "query' não pode ser vazia" in json_data["error"]


@pytest.mark.skipif(not SEARCH_ORCH_AVAILABLE, reason="SearchOrchestrator não pôde ser importado.")
def test_search_documents_no_results(client: FlaskClient, mocker):
    """Testa o endpoint /api/v1/search quando nenhum resultado é encontrado."""
    mocker.patch(
        'python_backend_services.app.services.search_orchestrator.SearchOrchestrator.search_and_rerank_documents',
        return_value=None  # Simula o orchestrator não retornando nada
    )
    query_data = {"query": "query muito específica que não acha nada"}
    response = client.post('/api/v1/search', json=query_data)

    assert response.status_code == 404  # Ou 200 com uma mensagem de "nada encontrado", depende da sua API
    json_data = response.get_json()
    assert "message" in json_data  # Ou "error"
    assert "Nenhum resultado adequado encontrado" in json_data["message"]

# Adicione mais testes para:
# - Erros internos do servidor (mockar o orchestrator para levantar uma exceção)
# - Diferentes tipos de queries e resultados esperados