# python_backend_services/data_ingestion/indexer_service.py
from elasticsearch import Elasticsearch, exceptions as es_exceptions
from elasticsearch.helpers import bulk
from typing import List, Dict, Any, Optional
import logging
import time

try:
    # Importação absoluta para quando executado como parte do pacote
    from python_backend_services.app.core.config import settings
except ImportError:
    settings = None
    # Este print só aparecerá se o import absoluto falhar (ex: ao executar este arquivo diretamente)
    print(
        "AVISO (indexer_service.py): Falha ao importar 'settings' via 'python_backend_services.app.core.config'. Verifique o PYTHONPATH ou a forma de execução se este não for um teste direto.")

# Configura o logging antes de qualquer uso, idealmente no ponto de entrada da aplicação (run_ingestion.py)
# Mas para permitir testes diretos deste módulo, configuramos aqui também se não foi feito.
if not logging.getLogger().hasHandlers():  # Evita adicionar handlers múltiplos se já configurado
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(name)s:%(lineno)d] - %(message)s')
logger = logging.getLogger(__name__)


class ElasticsearchService:
    def __init__(self, es_hosts: List[str], es_user: Optional[str] = None, es_password: Optional[str] = None):
        http_auth = None
        if es_user and es_password:
            http_auth = (es_user, es_password)
        try:
            self.es_client = Elasticsearch(
                hosts=es_hosts, http_auth=http_auth, retry_on_timeout=True, max_retries=3, timeout=30
            )
            # Testa a conexão imediatamente
            if not self.es_client.ping():
                # Se o ping falhar, levanta um ConnectionError claro
                raise ConnectionError(
                    f"Falha no PING ao cluster Elasticsearch em {es_hosts}. O servidor está acessível e rodando?")
            logger.info(f"Conectado com sucesso ao Elasticsearch em {es_hosts}")
        except ConnectionError as ce:  # Captura ConnectionError do self.es_client.ping() ou do construtor
            logger.error(f"Erro de Conexão com Elasticsearch: {ce}", exc_info=True)
            raise  # Re-lança para que o chamador (run_ingestion) possa tratar e parar a execução
        except Exception as e:  # Outras exceções durante a inicialização do cliente
            logger.error(f"Erro Inesperado ao inicializar Elasticsearch client: {e}", exc_info=True)
            raise

    def create_index_if_not_exists(self, index_name: str, embedding_dimensions: int,
                                   index_config_override: Optional[Dict[str, Any]] = None) -> None:
        if self.es_client.indices.exists(index=index_name):
            logger.info(f"Índice '{index_name}' já existe.")
            return

        final_index_config = index_config_override
        if final_index_config is None:
            final_index_config = {
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0,  # Para desenvolvimento local
                    "analysis": {
                        "analyzer": {
                            # Analyzer padrão para campos de texto não especificados, se houver
                            "default": {"type": "standard", "filter": ["lowercase", "asciifolding"]},
                            "portuguese_analyzer": {  # Analyzer específico para português
                                "type": "custom",
                                "tokenizer": "standard",  # Tokenizer padrão, bom para a maioria dos casos
                                "filter": [
                                    "lowercase",  # Converte para minúsculas
                                    "asciifolding",  # Remove acentos
                                    "portuguese_stop_filter",  # Remove stopwords em português
                                    "portuguese_stemmer_filter"  # Aplica stemming para português
                                ]
                            }
                        },
                        "filter": {
                            "portuguese_stop_filter": {  # Definição do filtro de stopwords
                                "type": "stop",
                                "stopwords": "_portuguese_"  # Lista embutida do ES para pt-BR
                            },
                            "portuguese_stemmer_filter": {  # Definição do filtro de stemming
                                "type": "stemmer",
                                "language": "portuguese"  # Stemmer embutido do ES para pt-BR
                            }
                        }
                    }
                },
                "mappings": {  # Mapeamento completo e corrigido
                    "properties": {
                        # IDs e Caminhos
                        "document_id": {"type": "keyword"},  # Do TSV, usado como _id
                        "file_name": {"type": "keyword"},
                        "content_path": {"type": "keyword"},  # Campo do TSV original
                        "content_path_resolved": {"type": "keyword"},  # Caminho completo resolvido pelo parser

                        # Campos de Classificação (principalmente para filtros exatos)
                        "document_category": {"type": "keyword"},
                        "document_type": {"type": "keyword"},
                        "legal_action": {"type": "keyword"},
                        "legal_domain": {"type": "keyword"},
                        "sub_areas_of_law": {"type": "keyword"},
                        # Elasticsearch lida bem com listas de strings para campos keyword
                        "jurisprudence_court": {"type": "keyword"},
                        "version": {"type": "keyword"},

                        # Campos Textuais para Busca e Display
                        "document_title": {"type": "text", "analyzer": "portuguese_analyzer",
                                           "fields": {"keyword": {"type": "keyword", "ignore_above": 512}}},
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
                    }
                }
            }
        try:
            self.es_client.indices.create(index=index_name, body=final_index_config)
            logger.info(f"Índice '{index_name}' criado com sucesso com o mapeamento especificado.")
        except es_exceptions.RequestError as e:
            # A biblioteca elasticsearch levanta RequestError para erros HTTP 400, etc.
            if hasattr(e, 'error') and e.error == 'resource_already_exists_exception':
                logger.info(f"Índice '{index_name}' já existe (detectado durante a criação).")
            else:
                # Log detalhado do erro do Elasticsearch
                error_info = e.info.get('error', {}) if hasattr(e, 'info') else {}
                root_cause = error_info.get('root_cause', [{}])[0].get('reason', str(e))  # Tenta pegar a causa raiz
                logger.error(
                    f"Falha ao criar índice '{index_name}': {getattr(e, 'error', 'N/A')} - Causa Raiz: {root_cause}",
                    exc_info=True)
                raise  # Re-lança para que o chamador possa tratar
        except Exception as e:  # Outras exceções inesperadas
            logger.error(f"Erro inesperado ao criar índice '{index_name}': {e}", exc_info=True)
            raise

    def bulk_index_documents(self, index_name: str, documents: List[Dict[str, Any]]) -> tuple[int, list]:
        if not documents:
            logger.info("Nenhum documento fornecido para indexação em lote.")
            return 0, []

        actions = []
        for doc_data_from_parser in documents:
            doc_id_for_es = doc_data_from_parser.get("id")
            if not doc_id_for_es:
                logger.warning(
                    f"Documento sem 'id' (esperado de 'document_id' do TSV). Pulando: {doc_data_from_parser.get('file_name', 'Nome de arquivo não disponível')}")
                continue

            # Garante que o campo 'content' seja uma string, mesmo que None, antes de enviar ao ES
            # já que campos de texto mapeados no ES não aceitam null.
            if doc_data_from_parser.get("content") is None:
                logger.warning(
                    f"Documento '{doc_id_for_es}' (file: {doc_data_from_parser.get('file_name')}) com 'content' None. Será indexado com campo 'content' como string vazia.")
                doc_data_from_parser["content"] = ""  # Indexa como string vazia

            # Prepara o _source payload. Inclui todos os campos do doc_data_from_parser, exceto 'id'.
            # Se um campo esperado pelo mapping não estiver no doc_data_from_parser, ele não será enviado
            # e o ES usará o default ou dará erro se o campo for obrigatório e não tiver default.
            # O mapeamento atual não tem campos obrigatórios além do que o parser já fornece.
            source_payload = {k: v for k, v in doc_data_from_parser.items() if k != "id"}

            actions.append({"_index": index_name, "_id": doc_id_for_es, "_source": source_payload})

        if not actions:
            logger.warning("Nenhum documento com ID válido encontrado para indexação em lote após filtragem.")
            return 0, []

        logger.info(f"Tentando indexar em lote {len(actions)} documentos em '{index_name}'...")
        try:
            # request_timeout aumentado para operações de bulk mais longas
            success_count, errors = bulk(self.es_client, actions, raise_on_error=False, raise_on_exception=False,
                                         request_timeout=120)
            logger.info(f"Indexação em lote concluída. Sucesso: {success_count} documentos.")
            if errors:
                logger.error(f"Erros ocorreram durante a indexação em lote: {len(errors)}")
                for i, error_detail in enumerate(errors[:5]):  # Log dos primeiros 5 erros detalhados
                    # O formato do erro de item de bulk pode variar, tenta extrair o máximo de info
                    item_response = error_detail.get('index', error_detail.get('create', error_detail.get('update',
                                                                                                          error_detail.get(
                                                                                                              'delete',
                                                                                                              {}))))

                    err_index = item_response.get('_index', 'N/A')
                    err_id = item_response.get('_id', 'N/A')
                    err_status = item_response.get('status', 'N/A')
                    err_reason = item_response.get('error', {}).get('reason', 'Razão desconhecida')
                    err_type = item_response.get('error', {}).get('type', 'Tipo desconhecido')
                    logger.error(
                        f"  Erro Detalhe {i + 1}: Índice: {err_index}, ID: {err_id}, Status: {err_status}, TipoErro: {err_type}, Razão: {err_reason}")
            return success_count, errors
        except es_exceptions.ElasticsearchException as e:  # Captura exceções específicas do Elasticsearch
            logger.error(f"Falha na operação de bulk do Elasticsearch: {e}", exc_info=True)
            # Retorna a estrutura de erro esperada pelo chamador
            return 0, [{"error_type": "ElasticsearchExceptionDuranteBulk", "reason": str(e)}]
        except Exception as e:  # Outras exceções inesperadas
            logger.error(f"Erro inesperado durante indexação em lote: {e}", exc_info=True)
            return 0, [{"error_type": "ExceçãoGenéricaNoBulk", "reason": str(e)}]

    def delete_index(self, index_name: str) -> bool:
        if not self.es_client.indices.exists(index=index_name):
            logger.info(f"Índice '{index_name}' não existe, não pode ser deletado.")
            return False
        try:
            self.es_client.indices.delete(index=index_name)
            logger.info(f"Índice '{index_name}' deletado com sucesso.")
            return True
        except Exception as e:
            logger.error(f"Falha ao deletar índice '{index_name}': {e}", exc_info=True)
            return False


# Bloco if __name__ == '__main__': (para teste direto, mantido como antes, mas adaptado para usar settings se possível)
if __name__ == '__main__':
    ES_HOSTS_TEST = ["http://localhost:9200"]
    ES_INDEX_TEST = "test_standalone_indexer_v4"  # Nome de índice para teste
    ES_USER_TEST = None
    ES_PASSWORD_TEST = None
    EMBEDDING_DIMS_TEST = 4096  # Padrão para Mistral 7B

    if settings:  # Se o import de python_backend_services.app.core.config funcionou
        ES_HOSTS_TEST = settings.ELASTICSEARCH_HOSTS
        # ES_INDEX_TEST = settings.ELASTICSEARCH_INDEX_NAME # Pode usar o mesmo do config
        ES_USER_TEST = settings.ELASTICSEARCH_USER
        ES_PASSWORD_TEST = settings.ELASTICSEARCH_PASSWORD
        EMBEDDING_DIMS_TEST = settings.EMBEDDING_DIMENSIONS
        logger.info(f"Usando settings do projeto para teste standalone do IndexerService: Índice '{ES_INDEX_TEST}'")
    else:
        logger.warning("Settings do projeto não carregadas. Usando padrões para teste standalone do IndexerService.")

    try:
        es_service = ElasticsearchService(es_hosts=ES_HOSTS_TEST, es_user=ES_USER_TEST, es_password=ES_PASSWORD_TEST)
        print(f"\n--- Teste Standalone ElasticsearchService ---")
        print(f"Usando Índice: '{ES_INDEX_TEST}' para este teste.")

        print(f"\nTentando deletar índice '{ES_INDEX_TEST}' (se existir)...")
        es_service.delete_index(ES_INDEX_TEST)
        time.sleep(1)

        print(f"\nTentando criar índice '{ES_INDEX_TEST}' com EMBEDDING_DIMS={EMBEDDING_DIMS_TEST}...")
        es_service.create_index_if_not_exists(ES_INDEX_TEST, embedding_dimensions=EMBEDDING_DIMS_TEST)

        dummy_docs_for_test = [
            {
                "id": "pet001-test-idx", "document_id": "pet001-test-idx", "file_name": "peticao_x.txt",
                "content_path": "peticao_x.txt", "content_path_resolved": "/fake/peticao_x.txt",
                "document_title": "Título da Petição de Teste X", "summary": "Sumário da petição X.",
                "first_lines": "Começo da petição X...", "document_category": "Petição",
                "document_type": "Petição Inicial", "legal_action": "Ação Indenizatória Teste",
                "legal_domain": "Direito Civil Teste", "sub_areas_of_law": "Responsabilidade Civil Teste",
                "jurisprudence_court": None, "version": "vTest",
                "content": "Este é o conteúdo completo da petição de teste X.",
                "content_embedding": None  # Será preenchido na Fase 3 do MVP
            }
        ]
        print(f"\nTentando indexar em lote {len(dummy_docs_for_test)} documentos dummy...")
        s_count, errs = es_service.bulk_index_documents(ES_INDEX_TEST, dummy_docs_for_test)
        print(f"Resultado da Indexação - Sucesso: {s_count}, Erros: {len(errs)}")
        if errs:
            logger.error(f"Detalhes dos erros na indexação durante o teste: {errs}")

        if s_count > 0:
            time.sleep(1)
            count_result = es_service.es_client.count(index=ES_INDEX_TEST)
            print(f"\nContagem de documentos no índice '{ES_INDEX_TEST}': {count_result.get('count')}")

    except ConnectionError as ce:
        # Este print será mais informativo se o Elasticsearch não estiver rodando
        print(
            f"\n❌ FALHA NA CONEXÃO DURANTE O TESTE: Não foi possível conectar ao Elasticsearch em {ES_HOSTS_TEST}. Detalhes: {ce}")
        print("   Verifique se o serviço Elasticsearch está em execução e acessível.")
    except Exception as e:
        print(f"\n❌ ERRO INESPERADO durante o teste standalone do ElasticsearchService: {e}", exc_info=True)