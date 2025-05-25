# python_backend_services/tests/test_search_orchestrator.py
# This file should be correct from v9 of the Canvas (where its 6 tests were passing)
import pytest
from unittest.mock import patch, MagicMock
from typing import List, Dict, Any, Optional

try:
    from python_backend_services.app.services.search_orchestrator import SearchOrchestrator
    from python_backend_services.data_ingestion.indexer_service import ElasticsearchService
    from python_backend_services.app.core.config import settings
except ImportError as e:
    print(f"ORCH_TEST_ERROR: Failed to import modules for test_search_orchestrator: {e}")


    class ElasticsearchService:
        pass


    class SearchOrchestrator:  # type: ignore
        def __init__(self): self.es_service = None; self.index_name = "dummy"; self.logger = MagicMock()

        def search_petitions_bm25_only(self, q, top_n=3): return []

        def get_document_details_by_id(self, id_): return None


    class DummySettingsOrchTest:
        ELASTICSEARCH_INDEX_NAME = "dummy_index_orch_test";
        BM25_TOP_N_RESULTS = 3;
        ELASTICSEARCH_HOSTS = ["http://dummy-es-orch-test"];
        ELASTICSEARCH_USER = None;
        ELASTICSEARCH_PASSWORD = None; LOG_LEVEL="DEBUG";
        LOG_LEVEL = "DEBUG"


    settings = DummySettingsOrchTest()
    SearchOrchestrator.logger = MagicMock()  # type: ignore


@pytest.fixture
def mock_es_service_for_orchestrator():
    mock_es = MagicMock(spec=ElasticsearchService)
    mock_es.es_client = MagicMock()

    mock_es.es_client.search.return_value = {
        'hits': {
            'hits': [
                {'_id': 'doc1_es_id', '_score': 1.5, '_source': {'id': 'doc1_source_id', 'file_name': 'doc1.txt',
                                                                 'content': 'Content of document 1 about apples.'}},
                {'_id': 'doc2_es_id', '_score': 1.2, '_source': {'id': 'doc2_source_id', 'file_name': 'doc2.txt',
                                                                 'content': 'Document 2 talks about bananas and apples.'}},
            ]
        }
    }

    mock_es_get_response_object = MagicMock(name="ESGetResponseObject")

    def es_response_object_get_side_effect(key_to_fetch):
        data_for_get = {
            '_source': {'id': 'doc_detail_id', 'file_name': 'detail.txt', 'content': 'Full detailed content.'},
            'found': True
        }
        return data_for_get.get(key_to_fetch)

    mock_es_get_response_object.get.side_effect = es_response_object_get_side_effect

    mock_es.es_client.get.return_value = mock_es_get_response_object

    mock_es.es_client.exists.return_value = True
    return mock_es


@pytest.fixture
def search_orchestrator_instance(mock_es_service_for_orchestrator, monkeypatch):
    monkeypatch.setattr(
        'python_backend_services.app.services.search_orchestrator.ElasticsearchService',
        lambda *args, **kwargs: mock_es_service_for_orchestrator
    )
    orchestrator = SearchOrchestrator()
    assert orchestrator.es_service == mock_es_service_for_orchestrator
    return orchestrator


def test_search_petitions_bm25_only_success(search_orchestrator_instance,
                                            mock_es_service_for_orchestrator):
    orchestrator = search_orchestrator_instance
    user_query = "apples"
    top_n_expected = settings.BM25_TOP_N_RESULTS

    results = orchestrator.search_petitions_bm25_only(user_query)

    mock_es_service_for_orchestrator.es_client.search.assert_called_once()
    actual_call_kwargs = mock_es_service_for_orchestrator.es_client.search.call_args.kwargs

    assert actual_call_kwargs['index'] == settings.ELASTICSEARCH_INDEX_NAME
    assert actual_call_kwargs['body']['query']['match']['content'] == user_query
    assert actual_call_kwargs['body']['size'] == top_n_expected
    assert "_source" in actual_call_kwargs['body']

    assert len(results) == 2
    assert results[0]['document_id'] == 'doc1_es_id'
    assert results[0]['file_name'] == 'doc1.txt'
    assert "apples" in results[0]['content_preview']
    assert results[0]['score'] == 1.5


def test_search_petitions_bm25_only_no_results(search_orchestrator_instance,
                                               mock_es_service_for_orchestrator):
    orchestrator = search_orchestrator_instance
    mock_es_service_for_orchestrator.es_client.search.return_value = {'hits': {'hits': []}}

    results = orchestrator.search_petitions_bm25_only("query with no results")
    assert len(results) == 0


def test_search_petitions_bm25_only_es_service_unavailable(search_orchestrator_instance,
                                                           mock_es_service_for_orchestrator):
    orchestrator = search_orchestrator_instance

    original_es_service = orchestrator.es_service

    if orchestrator.es_service:
        orchestrator.es_service.es_client = None
    results_no_client = orchestrator.search_petitions_bm25_only("query")
    assert results_no_client == []

    if original_es_service:
        orchestrator.es_service = original_es_service
        if hasattr(original_es_service, 'es_client'):
            orchestrator.es_service.es_client = original_es_service.es_client

    orchestrator.es_service = None
    results_no_service = orchestrator.search_petitions_bm25_only("query")
    assert results_no_service == []


def test_search_petitions_bm25_only_es_exception(search_orchestrator_instance,
                                                 mock_es_service_for_orchestrator):
    orchestrator = search_orchestrator_instance
    if orchestrator.es_service and orchestrator.es_service.es_client:
        orchestrator.es_service.es_client.search.side_effect = Exception("ES Down")
        results = orchestrator.search_petitions_bm25_only("any query")
        assert results == []
    else:
        pytest.skip("Skipping ES exception test as ES service or client was not available on orchestrator")


def test_get_document_details_by_id_success(search_orchestrator_instance,
                                            mock_es_service_for_orchestrator):
    orchestrator = search_orchestrator_instance
    doc_id = "doc_detail_id"

    expected_source = {'id': 'doc_detail_id', 'file_name': 'detail.txt', 'content': 'Full detailed content.'}

    if orchestrator.es_service and orchestrator.es_service.es_client:
        mock_response_obj = orchestrator.es_service.es_client.get.return_value

        def get_method_side_effect_specific(key_to_get):
            if key_to_get == '_source':
                return expected_source
            elif key_to_get == 'found':
                return True
            return None

        mock_response_obj.get.side_effect = get_method_side_effect_specific
    else:
        pytest.skip("Skipping get_document_details_by_id_success as ES service is not available")

    details = orchestrator.get_document_details_by_id(doc_id)

    mock_es_service_for_orchestrator.es_client.exists.assert_called_with(index=settings.ELASTICSEARCH_INDEX_NAME,
                                                                                id=doc_id)
    mock_es_service_for_orchestrator.es_client.get.assert_called_with(index=settings.ELASTICSEARCH_INDEX_NAME,
                                                                             id=doc_id)
    assert details is not None
    assert details['id'] == "doc_detail_id"
    assert details['content'] == "Full detailed content."


def test_get_document_details_by_id_not_found(search_orchestrator_instance,
                                              mock_es_service_for_orchestrator):
    orchestrator = search_orchestrator_instance
    doc_id = "non_existent_id"
    mock_es_service_for_orchestrator.es_client.exists.return_value = False

    details = orchestrator.get_document_details_by_id(doc_id)

    mock_es_service_for_orchestrator.es_client.exists.assert_called_with(index=settings.ELASTICSEARCH_INDEX_NAME,
                                                                                id=doc_id)
    mock_es_service_for_orchestrator.es_client.get.assert_not_called()
    assert details is None


