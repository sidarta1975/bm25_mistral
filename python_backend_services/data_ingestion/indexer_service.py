# python_backend_services/data_ingestion/indexer_service.py
from elasticsearch import Elasticsearch, exceptions as es_exceptions
from elasticsearch.helpers import bulk
from typing import List, Dict, Any, Optional
import logging
import time

# Supondo que settings venha de app.core.config
# Para execução standalone, um fallback será usado no if __name__ == '__main__'
try:
    from app.core.config import settings as app_settings  # Renomeado para evitar conflito
except ImportError:
    app_settings = None

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')
logger = logging.getLogger(__name__)


class ElasticsearchService:
    def __init__(self, es_hosts: List[str], es_user: Optional[str] = None, es_password: Optional[str] = None):
        http_auth = None
        if es_user and es_password:
            http_auth = (es_user, es_password)
        try:
            self.es_client = Elasticsearch(
                hosts=es_hosts,
                http_auth=http_auth,
                retry_on_timeout=True,
                max_retries=3,
                timeout=30  # Adicionado um timeout maior para operações como bulk
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

    def create_index_if_not_exists(self, index_name: str, embedding_dimensions: int,
                                   index_config: Optional[Dict[str, Any]] = None) -> None:
        """
        Creates an Elasticsearch index if it doesn't already exist with the new comprehensive mapping.

        Args:
            index_name (str): The name of the index to create.
            embedding_dimensions (int): The number of dimensions for the dense_vector (embedding).
            index_config (Optional[Dict[str, Any]]): Optional full index configuration. If None, default is used.
        """
        if self.es_client.indices.exists(index=index_name):
            logger.info(f"Index '{index_name}' already exists.")
            return

        if index_config is None:
            index_config = {
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0,
                    "analysis": {
                        "analyzer": {
                            "default": {  # Analyzer padrão para campos não especificados
                                "type": "custom",
                                "tokenizer": "standard",
                                "filter": ["lowercase", "asciifolding"]
                            },
                            "portuguese_analyzer": {
                                "type": "custom",
                                "tokenizer": "standard",
                                "filter": [
                                    "lowercase",
                                    "asciifolding",
                                    "portuguese_stop_filter",  # Definido abaixo
                                    "portuguese_stemmer_filter"  # Definido abaixo
                                ]
                            }
                        },
                        "filter": {
                            "portuguese_stop_filter": {
                                "type": "stop",
                                "stopwords": "_portuguese_"  # Usa a lista de stopwords embutida do ES para pt-br
                            },
                            "portuguese_stemmer_filter": {
                                "type": "stemmer",
                                "language": "portuguese"  # Usa o stemmer embutido do ES para pt-br
                            }
                        }
                    }
                },
                "mappings": {
                    "properties": {
                        # IDs e Caminhos
                        "document_id": {"type": "keyword"},  # Do TSV, usado como _id
                        "file_name": {"type": "keyword"},
                        "content_path_resolved": {"type": "keyword"},  # Caminho completo resolvido pelo parser

                        # Campos de Classificação (principalmente para filtros exatos)
                        "document_category": {"type": "keyword"},
                        "document_type": {"type": "keyword"},
                        "legal_action": {"type": "keyword"},
                        "legal_domain": {"type": "keyword"},
                        "sub_areas_of_law": {"type": "keyword"},  # Pode ser uma lista de keywords
                        "jurisprudence_court": {"type": "keyword"},
                        "version": {"type": "keyword"},

                        # Campos Textuais para Busca e Display
                        "document_title": {"type": "text", "analyzer": "portuguese_analyzer",
                                           "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
                        "summary": {"type": "text", "analyzer": "portuguese_analyzer"},
                        "first_lines": {"type": "text", "analyzer": "portuguese_analyzer"},
                        # Ou apenas keyword se não for para busca full-text

                        # Conteúdo Principal
                        "content": {"type": "text", "analyzer": "portuguese_analyzer"},

                        # Vetor de Embedding (para Fase 3 do MVP)
                        "content_embedding": {
                            "type": "dense_vector",
                            "dims": embedding_dimensions  # Será configurado via settings
                        }
                        # Se você tinha outros campos como "tags" ou "glossary_terms_found"
                        # e eles não vêm do novo TSV, eles foram removidos deste mapeamento.
                        # Adicione-os de volta se necessário.
                    }
                }
            }

        try:
            self.es_client.indices.create(index=index_name, body=index_config)
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
        if not documents:
            logger.info("No documents provided for bulk indexing.")
            return 0, []

        actions = []
        for doc in documents:
            # O DocumentParser já deve fornecer o campo "id" que é o "document_id" do TSV
            doc_id = doc.get("id")
            if not doc_id:
                logger.warning(
                    f"Document missing 'id' (expected from 'document_id' in TSV). Skipping: {doc.get('file_name', 'N/A')}")
                continue

            # Prepara o documento para o Elasticsearch, removendo o 'id' da fonte se ele já é usado como _id
            source_doc = doc.copy()
            # Não é estritamente necessário remover 'id' de _source se _id é o mesmo,
            # mas pode ser mais limpo.
            # if "id" in source_doc:
            #    del source_doc["id"]

            actions.append({
                "_index": index_name,
                "_id": doc_id,
                "_source": source_doc  # O DocumentParser já adiciona "content" aqui
            })

        if not actions:
            logger.warning("No documents with valid IDs found for bulk indexing after filtering.")
            return 0, []

        logger.info(f"Attempting to bulk index {len(actions)} documents into '{index_name}'...")
        try:
            success_count, errors = bulk(self.es_client, actions, raise_on_error=False, raise_on_exception=False,
                                         request_timeout=60)
            logger.info(f"Bulk indexing complete. Successfully indexed: {success_count} documents.")
            if errors:
                logger.error(f"Errors occurred during bulk indexing: {len(errors)}")
                for i, error_detail in enumerate(errors[:5]):
                    logger.error(f"Error {i + 1}: {error_detail}")
            return success_count, errors
        except es_exceptions.ElasticsearchException as e:
            logger.error(f"Elasticsearch bulk operation failed: {e}")
            return 0, [{"error_type": "ElasticsearchException", "reason": str(e)}]
        except Exception as e:
            logger.error(f"An unexpected error occurred during bulk indexing: {e}")
            return 0, [{"error_type": "GenericException", "reason": str(e)}]

    def delete_index(self, index_name: str) -> bool:
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


if __name__ == '__main__':
    # Example Usage (requires Elasticsearch to be running)

    # Configurações para o teste (simulando o que viria de app_settings)
    if app_settings:
        ES_HOSTS = app_settings.ELASTICSEARCH_HOSTS
        ES_INDEX = app_settings.ELASTICSEARCH_INDEX_NAME
        ES_USER = app_settings.ELASTICSEARCH_USER
        ES_PASSWORD = app_settings.ELASTICSEARCH_PASSWORD
        EMBEDDING_DIMS = app_settings.EMBEDDING_DIMENSIONS  # Pega do config
        logger.info(
            f"Using Elasticsearch settings from app.core.config: Hosts={ES_HOSTS}, Index={ES_INDEX}, EmbeddingDims={EMBEDDING_DIMS}")
    else:
        logger.warning("Could not import app.core.config.settings. Using default Elasticsearch config for testing.")
        ES_HOSTS = ["http://localhost:9200"]
        ES_INDEX = "test_petitions_full_index"
        ES_USER = None
        ES_PASSWORD = None
        EMBEDDING_DIMS = 4096  # Default para Mistral 7B, ajuste se necessário

    try:
        es_service = ElasticsearchService(es_hosts=ES_HOSTS, es_user=ES_USER, es_password=ES_PASSWORD)

        print(f"\nAttempting to delete index '{ES_INDEX}' if it exists...")
        es_service.delete_index(ES_INDEX)
        time.sleep(1)

        print(f"\nAttempting to create index '{ES_INDEX}' with EMBEDDING_DIMS={EMBEDDING_DIMS}...")
        es_service.create_index_if_not_exists(ES_INDEX, embedding_dimensions=EMBEDDING_DIMS)

        # Dummy documents com a nova estrutura (sem embeddings por enquanto, só para testar o mapeamento)
        dummy_documents_new_structure = [
            {
                "id": "doc_001_v2",  # Este 'id' será usado como _id no ES
                "document_id": "doc_001_v2",
                "file_name": "pet_alimentos_v2.txt",
                "content_path_resolved": "/path/to/pet_alimentos_v2.txt",
                "document_title": "Petição Inicial de Alimentos para Menor (v2)",
                "summary": "Esta é uma petição que visa garantir alimentos para um menor de idade, conforme a lei.",
                "first_lines": "EXCELENTÍSSIMO SENHOR DOUTOR JUIZ DE DIREITO DA VARA DE FAMÍLIA...",
                "document_category": "Petição",
                "document_type": "Petição Inicial",
                "legal_action": "Ação de Alimentos",
                "legal_domain": "Direito de Família",
                "sub_areas_of_law": ["Alimentos", "Direito Infantojuvenil"],  # Lista de keywords
                "jurisprudence_court": None,
                "version": "2.1",
                "content": "Conteúdo completo da petição de alimentos aqui... Lorem ipsum dolor sit amet..."
                # "content_embedding": [0.1, 0.2, ..., 0.N] # Vetor com EMBEDDING_DIMS elementos (para Fase 3)
            },
            {
                "id": "contrato_001_v2",
                "document_id": "contrato_001_v2",
                "file_name": "contrato_loc_v2.docx",  # Exemplo com docx, embora o parser espere txt
                "content_path_resolved": "/path/to/contrato_loc_v2.docx",
                "document_title": "Contrato de Locação Residencial Padrão (v2)",
                "summary": "Modelo de contrato de locação para fins residenciais.",
                "first_lines": "Pelo presente instrumento particular de contrato de locação...",
                "document_category": "Contrato",
                "document_type": "Contrato de Locação",
                "legal_action": None,  # Nulo para contratos
                "legal_domain": "Direito Imobiliário",
                "sub_areas_of_law": ["Locação"],
                "jurisprudence_court": None,
                "version": "1.0b",
                "content": "Cláusulas do contrato de locação... Lorem ipsum dolor sit amet..."
            }
        ]
        print(f"\nAttempting to bulk index {len(dummy_documents_new_structure)} dummy documents (new structure)...")
        success_count, errors = es_service.bulk_index_documents(ES_INDEX, dummy_documents_new_structure)
        print(f"Indexing result - Success: {success_count}, Errors: {len(errors)}")
        if errors:
            logger.error(f"First error during bulk indexing: {errors[0]}")

        if es_service.es_client.indices.exists(index=ES_INDEX) and success_count > 0:
            time.sleep(1)
            count_result = es_service.es_client.count(index=ES_INDEX)
            print(f"\nDocument count in index '{ES_INDEX}': {count_result.get('count')}")

            # Tentar buscar um documento para verificar
            # try:
            #     doc_check = es_service.es_client.get(index=ES_INDEX, id="doc_001_v2")
            #     if doc_check.get("found"):
            #         print("\nSuccessfully retrieved 'doc_001_v2':")
            #         import json
            #         print(json.dumps(doc_check['_source'], indent=2, ensure_ascii=False))
            # except Exception as e_get:
            #     print(f"Error getting doc_001_v2: {e_get}")

    except ConnectionError:
        print("\n❌ Could not connect to Elasticsearch. Please ensure it's running and accessible.")
    except Exception as e:
        print(f"\n❌ An unexpected error occurred during the example run: {e}", exc_info=True)