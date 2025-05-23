# python_backend_services/tests/test_search.py
import pytest
from unittest.mock import patch, MagicMock
from flask import current_app

MOCK_BM25_SEARCH_RESULTS = [
    {"document_id": "doc1_id", "file_name": "file1.txt", "content_preview": "Content of file 1...", "score": 1.8},
    {"document_id": "doc2_id", "file_name": "file2.txt", "content_preview": "Content of file 2...", "score": 1.5}
]

MOCK_DOCUMENT_DETAIL = {
    "id": "doc_abc",
    "file_name": "abc.txt",
    "content": "Full content of document abc."
}


def get_mock_orchestrator_from_current_app(app_instance) -> MagicMock:
    """Helper to get the mocked orchestrator from the app context."""
    orchestrator = app_instance.extensions.get('search_orchestrator')
    assert orchestrator is not None, "Mocked SearchOrchestrator not found in app.extensions. Check conftest.py and app_stage1 fixture."
    assert isinstance(orchestrator, MagicMock), "Orchestrator in app.extensions is not a MagicMock."
    # Corrected: Check the mock's internal _mock_name, which is set by the 'name' parameter in MagicMock constructor
    assert orchestrator._mock_name == "AppInitMockOrchestrator", \
        "Orchestrator in app.extensions does not have the expected _mock_name 'AppInitMockOrchestrator'."
    return orchestrator


def test_health_check_healthy(client, app):
    mock_orchestrator = get_mock_orchestrator_from_current_app(app)

    response = client.get('/health')
    assert response.status_code == 200
    json_data = response.get_json()
    assert json_data['status'] == 'healthy'
    assert json_data.get('elasticsearch_connection') == 'ok'


def test_health_check_es_down(client, app):
    mock_orchestrator = get_mock_orchestrator_from_current_app(app)
    mock_orchestrator.es_service.es_client.ping.return_value = False

    response = client.get('/health')
    assert response.status_code == 503
    json_data = response.get_json()
    assert json_data['status'] == 'unhealthy'
    assert json_data.get('elasticsearch_connection') == 'error_ping_failed'


def test_search_endpoint_success(client, app):
    mock_orchestrator = get_mock_orchestrator_from_current_app(app)
    mock_orchestrator.search_petitions_bm25_only.reset_mock()
    mock_orchestrator.search_petitions_bm25_only.return_value = MOCK_BM25_SEARCH_RESULTS

    response = client.post('/api/v1/search', json={'query': 'test search query'})

    assert response.status_code == 200
    json_data = response.get_json()
    assert isinstance(json_data, list)
    assert len(json_data) == len(MOCK_BM25_SEARCH_RESULTS)
    mock_orchestrator.search_petitions_bm25_only.assert_called_once_with('test search query')


def test_search_endpoint_no_results(client, app):
    mock_orchestrator = get_mock_orchestrator_from_current_app(app)
    mock_orchestrator.search_petitions_bm25_only.reset_mock()
    mock_orchestrator.search_petitions_bm25_only.return_value = []

    response = client.post('/api/v1/search', json={'query': 'query for no results'})

    assert response.status_code == 404
    json_data = response.get_json()
    assert json_data['message'] == "No documents found matching your query"
    mock_orchestrator.search_petitions_bm25_only.assert_called_once_with('query for no results')


def test_search_endpoint_missing_query(client, app):
    mock_orchestrator = get_mock_orchestrator_from_current_app(app)
    mock_orchestrator.search_petitions_bm25_only.reset_mock()

    response = client.post('/api/v1/search', json={})

    assert response.status_code == 400
    json_data = response.get_json()
    assert json_data['error'] == "Missing 'query' in request body"
    mock_orchestrator.search_petitions_bm25_only.assert_not_called()


def test_get_document_endpoint_success(client, app):
    mock_orchestrator = get_mock_orchestrator_from_current_app(app)
    mock_orchestrator.get_document_details_by_id.reset_mock()
    doc_id_to_get = MOCK_DOCUMENT_DETAIL['id']
    mock_orchestrator.get_document_details_by_id.return_value = MOCK_DOCUMENT_DETAIL

    response = client.get(f'/api/v1/document/{doc_id_to_get}')

    assert response.status_code == 200
    json_data = response.get_json()
    assert json_data['id'] == doc_id_to_get
    mock_orchestrator.get_document_details_by_id.assert_called_once_with(doc_id_to_get)


def test_get_document_endpoint_not_found(client, app):
    mock_orchestrator = get_mock_orchestrator_from_current_app(app)
    mock_orchestrator.get_document_details_by_id.reset_mock()
    doc_id_not_found = "non_existent_document_id"
    mock_orchestrator.get_document_details_by_id.return_value = None

    response = client.get(f'/api/v1/document/{doc_id_not_found}')

    assert response.status_code == 404
    json_data = response.get_json()
    assert json_data['error'] == f"Document with ID '{doc_id_not_found}' not found"
    mock_orchestrator.get_document_details_by_id.assert_called_once_with(doc_id_not_found)


def test_search_api_orchestrator_es_truly_unavailable(client, app):
    mock_orchestrator = get_mock_orchestrator_from_current_app(app)
    mock_orchestrator.search_petitions_bm25_only.reset_mock()
    mock_orchestrator.es_service = None

    response = client.post('/api/v1/search', json={'query': 'test query'})

    assert response.status_code == 503
    json_data = response.get_json()
    assert "Search service temporarily unavailable" in json_data['error']
    mock_orchestrator.search_petitions_bm25_only.assert_not_called()
