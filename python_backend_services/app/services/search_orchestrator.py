# python_backend_services/app/services/search_orchestrator.py
# Simplified for Stage 1: Only BM25 search, no LLM, no query enrichment.
import logging
from typing import List, Dict, Optional, Any

# Assuming ElasticsearchService is correctly placed and importable
# You might need to adjust the import path based on your exact project structure
# and how you run the application (e.g., if python_backend_services is a top-level package).
try:
    from python_backend_services.data_ingestion.indexer_service import ElasticsearchService
    from python_backend_services.app.core.config import settings
except ImportError as e:
    logging.basicConfig(level="CRITICAL")
    logging.critical(
        f"CRITICAL: Failed to import necessary modules for SearchOrchestrator (Stage 1): {e}. Ensure paths are correct and __init__.py files exist.")
    raise

logger = logging.getLogger(__name__)


class SearchOrchestrator:
    def __init__(self):
        self.es_service: Optional[ElasticsearchService] = None
        self.index_name: str = settings.ELASTICSEARCH_INDEX_NAME
        try:
            self.es_service = ElasticsearchService(
                es_hosts=settings.ELASTICSEARCH_HOSTS,
                es_user=settings.ELASTICSEARCH_USER,
                es_password=settings.ELASTICSEARCH_PASSWORD
            )
            logger.info("SearchOrchestrator (Stage 1) initialized with ElasticsearchService.")
        except ConnectionError as e:
            logger.critical(
                f"SearchOrchestrator (Stage 1): Failed to connect to Elasticsearch during init - {e}. Search functionality will be impaired.")
            # self.es_service remains None
        except Exception as e:
            logger.critical(
                f"SearchOrchestrator (Stage 1): Unexpected error initializing ElasticsearchService - {e}. Search functionality will be impaired.")
            self.es_service = None  # Ensure es_service is None for other init errors

    def search_petitions_bm25_only(self, user_query: str, top_n: int = settings.BM25_TOP_N_RESULTS) -> List[
        Dict[str, Any]]:
        """
        Performs a direct BM25 search in Elasticsearch based on the user query.
        No LLM reranking or query enrichment in this stage.
        """
        if not self.es_service or not self.es_service.es_client:
            logger.error("Elasticsearch service is not available for search_petitions_bm25_only.")
            return []

        logger.info(f"Stage 1 Search: Performing BM25 search for query='{user_query}', top_n={top_n}")

        # Basic Elasticsearch query for full-text search on the 'content' field.
        # BM25 is the default similarity algorithm for text fields in Elasticsearch.
        query_body: Dict[str, Any] = {
            "query": {
                "match": {
                    "content": user_query  # Assuming 'content' is the main text field indexed
                }
            },
            "size": top_n,
            # Request specific fields from _source if needed, otherwise all are returned.
            # For MVP, returning id, file_name, and a snippet of content might be good.
            "_source": ["id", "file_name", "content"]
        }

        try:
            response = self.es_service.es_client.search(index=self.index_name, body=query_body)

            results = []
            for hit in response.get('hits', {}).get('hits', []):
                source_data = hit.get('_source', {})
                # Ensure the ID from the hit is used, as it's the definitive ES document ID
                result_item = {
                    "document_id": hit.get('_id'),
                    "file_name": source_data.get("file_name"),
                    # For MVP, return a preview or full content based on what's needed for display/next steps
                    "content_preview": source_data.get("content", "")[:500] + "..." if source_data.get(
                        "content") else "",
                    "score": hit.get('_score')  # BM25 score
                }
                results.append(result_item)

            logger.info(f"BM25 search found {len(results)} results.")
            return results
        except Exception as e:
            logger.error(f"Error during Elasticsearch BM25 search in search_petitions_bm25_only: {e}", exc_info=True)
            return []

    def get_document_details_by_id(self, document_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieves a full document by its ID from Elasticsearch.
        (This method can be kept from the more complete orchestrator if needed for a /document endpoint)
        """
        if not self.es_service or not self.es_service.es_client:
            logger.error("Elasticsearch service is not available for get_document_details_by_id.")
            return None

        logger.info(f"Fetching document details for ID: {document_id} from index {self.index_name}")
        try:
            if self.es_service.es_client.exists(index=self.index_name, id=document_id):
                response = self.es_service.es_client.get(index=self.index_name, id=document_id)
                # The _source field contains the original document JSON
                return response.get('_source') if response.get('found') else None
            else:
                logger.warning(f"Document ID '{document_id}' not found in index '{self.index_name}'.")
                return None
        except Exception as e:
            logger.error(f"Error retrieving document {document_id} from Elasticsearch: {e}", exc_info=True)
            return None



