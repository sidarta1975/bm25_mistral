# python_backend_services/data_ingestion/indexer_service.py
from elasticsearch import Elasticsearch, exceptions as es_exceptions
from elasticsearch.helpers import bulk
from typing import List, Dict, Any, Optional
import logging
import time

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')
logger = logging.getLogger(__name__)


class ElasticsearchService:
    def __init__(self, es_hosts: List[str], es_user: Optional[str] = None, es_password: Optional[str] = None):
        """
        Initializes the Elasticsearch client.

        Args:
            es_hosts (List[str]): List of Elasticsearch node URLs (e.g., ["http://localhost:9200"]).
            es_user (Optional[str]): Username for Elasticsearch basic authentication.
            es_password (Optional[str]): Password for Elasticsearch basic authentication.
        """
        http_auth = None
        if es_user and es_password:
            http_auth = (es_user, es_password)

        try:
            self.es_client = Elasticsearch(
                hosts=es_hosts,
                http_auth=http_auth,
                retry_on_timeout=True,
                max_retries=3
            )
            if not self.es_client.ping():
                raise ConnectionError("Failed to connect to Elasticsearch cluster.")
            logger.info(f"Successfully connected to Elasticsearch at {es_hosts}")
        except ConnectionError as ce:
            logger.error(f"Elasticsearch connection error: {ce}")
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred during Elasticsearch client initialization: {e}")
            raise

    def create_index_if_not_exists(self, index_name: str, index_settings: Optional[Dict[str, Any]] = None) -> None:
        """
        Creates an Elasticsearch index if it doesn't already exist.

        Args:
            index_name (str): The name of the index to create.
            index_settings (Optional[Dict[str, Any]]): Custom settings and mappings for the index.
        """
        if self.es_client.indices.exists(index=index_name):
            logger.info(f"Index '{index_name}' already exists.")
            return

        if index_settings is None:
            # Define a default mapping if none is provided
            # This is a very basic mapping, customize it for your needs (analyzers, field types, etc.)
            index_settings = {
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0,  # For local development, 0 replicas is fine
                    "analysis": {
                        "analyzer": {
                            "default": {  # Using 'default' to override the standard analyzer
                                "type": "custom",
                                "tokenizer": "standard",
                                "filter": ["lowercase", "asciifolding"]  # Basic filters
                            },
                            "portuguese_analyzer": {  # Example specific analyzer for Portuguese
                                "type": "custom",
                                "tokenizer": "standard",
                                "filter": [
                                    "lowercase",
                                    "asciifolding",
                                    # "portuguese_stop", # Requires stop filter setup
                                    # "portuguese_stemmer" # Requires stemmer filter setup
                                ]
                            }
                        }
                        # Define filters like 'portuguese_stop', 'portuguese_stemmer' if needed
                        # "filter": {
                        #     "portuguese_stop": {
                        #         "type": "stop",
                        #         "stopwords": "_portuguese_"
                        #     },
                        #     "portuguese_stemmer": {
                        #         "type": "stemmer",
                        #         "language": "portuguese"
                        #     }
                        # }
                    }
                },
                "mappings": {
                    "properties": {
                        "id": {"type": "keyword"},  # Document ID from filename
                        "file_name": {"type": "keyword"},
                        "file_path": {"type": "keyword"},
                        "content": {
                            "type": "text",
                            "analyzer": "portuguese_analyzer"  # Use specific analyzer for content
                            # "analyzer": "standard" # Or a simpler one
                        },
                        "tags": {"type": "keyword"},
                        # Tags are usually treated as keywords for exact matching/filtering
                        "glossary_terms_found": {"type": "keyword"},
                        "area_of_law": {"type": "keyword"},
                        # Add other fields and their types as needed
                        # "embedding": {"type": "dense_vector", "dims": 768} # Example for vector embeddings
                    }
                }
            }

        try:
            self.es_client.indices.create(index=index_name, body=index_settings)
            logger.info(f"Index '{index_name}' created successfully with specified mappings.")
        except es_exceptions.RequestError as e:
            if e.error == 'resource_already_exists_exception':
                logger.info(f"Index '{index_name}' already exists (caught during create).")
            else:
                logger.error(f"Failed to create index '{index_name}': {e}")
                raise
        except Exception as e:
            logger.error(f"An unexpected error occurred while creating index '{index_name}': {e}")
            raise

    def bulk_index_documents(self, index_name: str, documents: List[Dict[str, Any]]) -> tuple[int, list]:
        """
        Indexes a list of documents into Elasticsearch using the bulk API.
        Each document dictionary must have an "id" key to be used as the Elasticsearch document ID.

        Args:
            index_name (str): The name of the index.
            documents (List[Dict[str, Any]]): A list of document dictionaries to index.

        Returns:
            tuple[int, list]: Number of successfully indexed documents, and a list of errors.
        """
        if not documents:
            logger.info("No documents provided for bulk indexing.")
            return 0, []

        actions = [
            {
                "_index": index_name,
                "_id": doc.get("id"),  # Use the "id" field from your document data
                "_source": doc
            }
            for doc in documents if doc.get("id")  # Ensure document has an ID
        ]

        if not actions:
            logger.warning("No documents with valid IDs found for bulk indexing.")
            return 0, []

        logger.info(f"Attempting to bulk index {len(actions)} documents into '{index_name}'...")
        try:
            success_count, errors = bulk(self.es_client, actions, raise_on_error=False, raise_on_exception=False)
            logger.info(f"Bulk indexing complete. Successfully indexed: {success_count} documents.")
            if errors:
                logger.error(f"Errors occurred during bulk indexing: {len(errors)}")
                for i, error in enumerate(errors[:5]):  # Log first 5 errors
                    logger.error(f"Error {i + 1}: {error}")
            return success_count, errors
        except es_exceptions.ElasticsearchException as e:
            logger.error(f"Elasticsearch bulk operation failed: {e}")
            return 0, [str(e)]  # Return the exception as an error
        except Exception as e:
            logger.error(f"An unexpected error occurred during bulk indexing: {e}")
            return 0, [str(e)]

    def delete_index(self, index_name: str) -> bool:
        """Deletes an Elasticsearch index."""
        if not self.es_client.indices.exists(index=index_name):
            logger.info(f"Index '{index_name}' does not exist, cannot delete.")
            return False
        try:
            self.es_client.indices.delete(index=index_name)
            logger.info(f"Index '{index_name}' deleted successfully.")
            return True
        except Exception as e:
            logger.error(f"Failed to delete index '{index_name}': {e}")
            return False

    # Add other methods as needed (e.g., search, get_document_by_id, update_document)
    # These will be used by the `search_orchestrator.py` in the `app/services` module.


if __name__ == '__main__':
    # Example Usage (requires Elasticsearch to be running)
    import sys
    import os

    # sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

    try:
        from app.core.config import settings

        ES_HOSTS = settings.ELASTICSEARCH_HOSTS
        ES_INDEX = settings.ELASTICSEARCH_INDEX_NAME
        ES_USER = settings.ELASTICSEARCH_USER
        ES_PASSWORD = settings.ELASTICSEARCH_PASSWORD
        print(f"Using Elasticsearch settings: Hosts={ES_HOSTS}, Index={ES_INDEX}")
    except ImportError:
        print("Could not import settings. Using default Elasticsearch config for testing.")
        ES_HOSTS = ["http://localhost:9200"]
        ES_INDEX = "test_petitions_index"
        ES_USER = None
        ES_PASSWORD = None

    try:
        es_service = ElasticsearchService(es_hosts=ES_HOSTS, es_user=ES_USER, es_password=ES_PASSWORD)

        # 1. Optionally delete the index if it exists (for a clean test run)
        print(f"\nAttempting to delete index '{ES_INDEX}' if it exists...")
        es_service.delete_index(ES_INDEX)
        time.sleep(1)  # Give ES a moment

        # 2. Create the index with specific mappings
        print(f"\nAttempting to create index '{ES_INDEX}'...")
        es_service.create_index_if_not_exists(ES_INDEX)  # Default mappings will be used if not specified

        # 3. Prepare some dummy documents for indexing
        dummy_documents = [
            {"id": "doc_001", "file_name": "pet_alimentos.txt",
             "content": "Petição inicial de ação de alimentos para menor.", "tags": ["alimentos", "família"],
             "glossary_terms_found": ["alimentos", "pensão alimentícia"], "area_of_law": "Familia"},
            {"id": "doc_002", "file_name": "contrato_compra_venda.txt",
             "content": "Contrato de compra e venda de imóvel residencial.", "tags": ["contrato", "imóvel", "civil"],
             "glossary_terms_found": ["contrato de compra e venda"], "area_of_law": "Civil"},
            {"id": "doc_003", "file_name": "defesa_usucapiao.txt",
             "content": "Peça de defesa em processo de usucapião especial urbano.",
             "tags": ["usucapião", "defesa", "imóvel"], "glossary_terms_found": ["usucapião"], "area_of_law": "Civil"}
        ]
        print(f"\nAttempting to bulk index {len(dummy_documents)} dummy documents...")
        success_count, errors = es_service.bulk_index_documents(ES_INDEX, dummy_documents)
        print(f"Indexing result - Success: {success_count}, Errors: {len(errors)}")
        if errors:
            print("First error:", errors[0])

        # Verify by checking index count (optional)
        if es_service.es_client.indices.exists(index=ES_INDEX):
            time.sleep(1)  # Allow time for indexing to settle
            count_result = es_service.es_client.count(index=ES_INDEX)
            print(f"\nDocument count in index '{ES_INDEX}': {count_result.get('count')}")

    except ConnectionError:
        print("\n❌ Could not connect to Elasticsearch. Please ensure it's running and accessible.")
    except Exception as e:
        print(f"\n❌ An unexpected error occurred during the example run: {e}")

