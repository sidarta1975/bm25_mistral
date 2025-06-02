# python_backend_services/data_ingestion/indexer_service.py
from elasticsearch import Elasticsearch, exceptions as es_exceptions
from elasticsearch.helpers import bulk
from typing import List, Dict, Any, Optional, Tuple
import logging
import time
import datetime  # Importar datetime para o bloco __main__

try:
    from python_backend_services.app.core.config import settings
except ImportError:
    print("indexer_service.py: WARNING - Could not import 'settings'. Using fallback.")


    class MockSettingsIndexer:
        ELASTICSEARCH_HOSTS = ["http://localhost:9200"]
        ELASTICSEARCH_USER = None
        ELASTICSEARCH_PASSWORD = None
        ELASTICSEARCH_REQUEST_TIMEOUT = 30
        EMBEDDING_DIMENSIONS = 4096
        LOG_LEVEL = "DEBUG"


    settings = MockSettingsIndexer()
    if not logging.getLogger().hasHandlers():
        logging.basicConfig(level=settings.LOG_LEVEL.upper(),
                            format='%(asctime)s - %(levelname)s - [%(name)s:%(lineno)d] - %(message)s')

logger = logging.getLogger(__name__)


class ElasticsearchService:
    def __init__(self,
                 es_hosts: Optional[List[str]] = None,
                 es_user: Optional[str] = None,
                 es_password: Optional[str] = None,
                 request_timeout: Optional[int] = None):

        self.hosts = es_hosts or settings.ELASTICSEARCH_HOSTS
        self.user = es_user or settings.ELASTICSEARCH_USER
        self.password = es_password or settings.ELASTICSEARCH_PASSWORD
        self.request_timeout = request_timeout or settings.ELASTICSEARCH_REQUEST_TIMEOUT

        auth_details = (self.user, self.password) if self.user and self.password else None

        try:
            self.es_client = Elasticsearch(
                hosts=self.hosts,
                basic_auth=auth_details,  # ATUALIZADO de http_auth para basic_auth
                retry_on_timeout=True,
                max_retries=3,
                request_timeout=self.request_timeout  # ATUALIZADO de timeout para request_timeout
            )
            if not self.es_client.ping():
                raise es_exceptions.ConnectionError(f"Falha no PING ao Elasticsearch em {self.hosts}.")
            logger.info(f"Conectado com sucesso ao Elasticsearch em {self.hosts}")
        except es_exceptions.ConnectionError as e:
            logger.critical(f"Não foi possível conectar ao Elasticsearch: {e}", exc_info=True)
            raise
        except Exception as e_gen:
            logger.critical(f"Erro inesperado ao inicializar ElasticsearchService: {e_gen}", exc_info=True)
            raise

    def create_index_if_not_exists(self, index_name: str, embedding_dimensions: Optional[int] = None) -> None:
        if embedding_dimensions is None:
            embedding_dimensions = settings.EMBEDDING_DIMENSIONS

        if self.es_client.indices.exists(index=index_name):
            logger.info(f"Índice '{index_name}' já existe.")
            return

        logger.info(f"Índice '{index_name}' não encontrado. Criando...")

        # Formatos de data aceitos pelo Elasticsearch, incluindo o formato do SQLite
        date_format_pattern = "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd'T'HH:mm:ss.SSSZ||yyyy-MM-dd'T'HH:mm:ssZ||epoch_millis||strict_date_optional_time"

        mapping = {
            "properties": {
                "document_id": {"type": "keyword"},
                "file_name": {"type": "keyword"},
                "content_path": {"type": "keyword"},
                "document_title_original": {"type": "text", "analyzer": "brazilian"},
                "summary_original": {"type": "text", "analyzer": "brazilian"},
                "first_lines_original": {"type": "text", "analyzer": "brazilian", "index": False},
                "document_category_original": {"type": "keyword"},
                "document_type_original": {"type": "keyword"},
                "legal_action_original": {"type": "keyword"},
                "legal_domain_original": {"type": "keyword"},
                "sub_areas_of_law_original": {"type": "keyword"},
                "jurisprudence_court_original": {"type": "keyword"},
                "version_original": {"type": "keyword"},
                "full_text_content": {"type": "text", "analyzer": "brazilian"},
                "document_title_llm": {"type": "text", "analyzer": "brazilian"},
                "summary1_llm": {"type": "text", "analyzer": "brazilian"},
                "summary2_llm": {"type": "text", "analyzer": "brazilian"},
                "legal_domain_llm": {"type": "keyword"},
                "sub_areas_of_law_llm": {"type": "keyword"},
                "document_specific_terms": {"type": "keyword"},
                "status_enrichment": {"type": "keyword"},
                "created_at": {"type": "date", "format": date_format_pattern},  # ATUALIZADO formato da data
                "updated_at": {"type": "date", "format": date_format_pattern},  # ATUALIZADO formato da data
                "content_embedding": {
                    "type": "dense_vector",
                    "dims": embedding_dimensions,
                    "index": True,
                    "similarity": "cosine"
                }
            }
        }
        try:
            self.es_client.indices.create(index=index_name, mappings=mapping)
            logger.info(f"Índice '{index_name}' criado com sucesso com o mapeamento especificado.")
        except es_exceptions.RequestError as e:
            if e.error == 'resource_already_exists_exception':
                logger.info(f"Índice '{index_name}' foi criado por outro processo.")
            else:
                logger.error(f"Erro ao criar o índice '{index_name}': {e.info}", exc_info=True)
                raise
        except Exception as e_gen:
            logger.error(f"Erro inesperado ao criar índice '{index_name}': {e_gen}", exc_info=True)
            raise

    def _create_bulk_actions(self, index_name: str, documents: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        actions = []
        for doc_dict in documents:
            doc_id_es = doc_dict.get("document_id")
            if not doc_id_es:
                logger.warning(
                    f"Documento sem 'document_id' encontrado, pulando: {doc_dict.get('file_name', 'N/A')}")
                continue
            source_doc = doc_dict.copy()
            if "content_embedding" in source_doc and source_doc["content_embedding"] is None:
                del source_doc["content_embedding"]
            action = {
                "_index": index_name,
                "_id": str(doc_id_es),
                "_source": source_doc
            }
            actions.append(action)
        return actions

    def bulk_index_documents(self, index_name: str, documents: List[Dict[str, Any]]) -> Tuple[
        int, List[Dict[str, Any]]]:
        if not documents:
            logger.warning("Nenhum documento fornecido para indexação em bulk.")
            return 0, []
        actions = self._create_bulk_actions(index_name, documents)
        if not actions:
            logger.warning("Nenhuma ação de bulk válida criada.")
            return 0, []

        logger.info(f"Tentando indexar em bulk {len(actions)} documentos no índice '{index_name}'...")
        success_count = 0
        errors_list = []
        try:
            s_count, errs = bulk(
                client=self.es_client,
                actions=actions,
                raise_on_error=False,
                raise_on_exception=False,
                request_timeout=self.request_timeout * 2
            )
            success_count = s_count
            errors_list = errs
            logger.info(f"Resultado da indexação em bulk: Sucessos = {success_count}, Erros = {len(errors_list)}")
            if errors_list:
                logger.warning(f"Houveram {len(errors_list)} erros durante a indexação em bulk. Primeiros 5 erros:")
                for i, err_detail in enumerate(errors_list[:5]):
                    action_type = list(err_detail.keys())[0]
                    item_response = err_detail.get(action_type, {})
                    error_info = item_response.get('error', {})
                    logger.error(
                        f"  Erro Detalhe {i + 1}: "
                        f"ID: {item_response.get('_id', 'N/A')}, "
                        f"Status: {item_response.get('status', 'N/A')}, "
                        f"Tipo Erro: {error_info.get('type', 'N/A')}, "
                        f"Razão: {error_info.get('reason', 'N/A')}"
                    )
            return success_count, errors_list
        except es_exceptions.ElasticsearchException as e:
            logger.error(f"Exceção do Elasticsearch durante a operação de bulk: {e}", exc_info=True)
            return 0, [
                {"error_type": "ElasticsearchBulkException", "reason": str(e), "details": getattr(e, 'info', None)}]
        except Exception as e_gen:
            logger.error(f"Erro crítico inesperado durante a operação de bulk: {e_gen}", exc_info=True)
            return 0, [{"error_type": "GenericBulkException", "reason": str(e_gen)}]

    def delete_index(self, index_name: str) -> bool:
        if not self.es_client.indices.exists(index=index_name):
            logger.info(f"Índice '{index_name}' não existe, nada a deletar.")
            return False
        try:
            self.es_client.indices.delete(index=index_name)
            logger.info(f"Índice '{index_name}' deletado com sucesso.")
            return True
        except Exception as e:
            logger.error(f"Erro ao deletar o índice '{index_name}': {e}", exc_info=True)
            return False


if __name__ == '__main__':
    if not logging.getLogger().hasHandlers():
        logging.basicConfig(level="DEBUG", format='%(asctime)s - %(levelname)s - [%(name)s:%(lineno)d] - %(message)s')
    logger.info("--- Testando ElasticsearchService Standalone ---")
    # ... (resto do bloco if __name__ == '__main__', que usa datetime.now(), por isso o import de datetime) ...
    # ... (ele já inclui a importação de json para sample_docs_for_indexing) ...

    # Exemplo de como o bloco if __name__ == '__main__' poderia ser:
    es_service_instance = None
    try:
        es_service_instance = ElasticsearchService()
        logger.info("ElasticsearchService instanciado para teste.")
    except Exception as e_init:
        logger.critical(f"Falha ao instanciar ElasticsearchService para teste: {e_init}", exc_info=True)

    if es_service_instance:
        test_index_name = "test_petitions_standalone_index"
        logger.info(f"Testando criação/deleção do índice: {test_index_name}")
        es_service_instance.delete_index(test_index_name)
        time.sleep(1)
        es_service_instance.create_index_if_not_exists(test_index_name,
                                                       embedding_dimensions=settings.EMBEDDING_DIMENSIONS)

        sample_docs = [
            {
                "document_id": "sa_doc_001", "file_name": "sa_test1.txt",
                "document_title_original": "Petição Standalone", "summary_original": "Resumo SA 1",
                "full_text_content": "Conteúdo completo da petição SA 1 com danos morais.",
                "summary1_llm": "Resumo técnico LLM para SA doc 1.",
                "document_specific_terms": ["danos morais", "teste sa"],  # Já como lista
                "status_enrichment": "enriched",
                "created_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # Formato correto
                "updated_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Formato correto
            }
        ]
        # Corrigir document_specific_terms para ser string JSON se _create_bulk_actions não for alterado
        # ou manter como lista e garantir que o ES aceite. O mapeamento é 'keyword', então uma lista de strings é ok.
        # No entanto, run_ingestion_pipeline lida com strings JSON do SQLite.
        # Para este teste standalone, vamos manter como lista de strings, pois o _create_bulk_actions lida bem.

        if es_service_instance.es_client.indices.exists(index=test_index_name):
            success_c, errors_l = es_service_instance.bulk_index_documents(test_index_name, sample_docs)
            logger.info(f"Indexação em bulk: Sucessos={success_c}, Erros={len(errors_l)}")
        # es_service_instance.delete_index(test_index_name) # Opcional: limpar após teste
    logger.info("--- Testes Standalone do ElasticsearchService Concluídos ---")