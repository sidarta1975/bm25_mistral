# python_backend_services/data_ingestion/run_ingestion.py
import logging
import time
import argparse
import os

# Imports usando o caminho completo a partir do pacote 'python_backend_services'
# Isso é crucial quando você executa com `python -m python_backend_services.data_ingestion.run_ingestion`
# a partir do diretório raiz do projeto (`bm25_mistral`).
try:
    from python_backend_services.app.core.config import settings
    from python_backend_services.data_ingestion.document_parser import DocumentParser  # Importa a CLASSE
    from python_backend_services.data_ingestion.indexer_service import ElasticsearchService
    # Se você reintroduzir glossary_processor e tag_extractor, use o mesmo padrão:
    # from python_backend_services.data_ingestion.glossary_processor import load_glossary_terms, process_documents_with_glossary
    # from python_backend_services.data_ingestion.tag_extractor import process_documents_for_tags
except ImportError as e:
    # Este bloco ajuda a diagnosticar problemas de importação se eles ainda ocorrerem.
    print(f"ERRO DE IMPORTAÇÃO em run_ingestion.py: {e}")
    print("Verifique se:")
    print("1. Você está executando este script a partir do diretório raiz do projeto 'bm25_mistral' usando o comando:")
    print("   python -m python_backend_services.data_ingestion.run_ingestion")
    print(
        "2. Todos os diretórios de pacotes (python_backend_services, app, app/core, data_ingestion) contêm um arquivo __init__.py.")
    print(
        f"3. PYTHONPATH (se configurado) está correto. Python sys.path atual: {sys.path}")  # Import sys para usar sys.path
    import sys  # Importa sys aqui para o print acima

    raise  # Re-lança a exceção para parar a execução

# Configura o logging usando o nível definido em settings
# É importante configurar o logging ANTES de usá-lo nos módulos importados
logging.basicConfig(level=settings.LOG_LEVEL.upper(),
                    format='%(asctime)s - %(levelname)s - [%(name)s:%(lineno)d] - %(message)s')
logger = logging.getLogger(__name__)  # Logger específico para este módulo


def main(should_recreate_index: bool):
    logger.info(f"Iniciando pipeline de ingestão de dados... Recriar índice: {should_recreate_index}")
    overall_start_time = time.time()

    # 1. Inicializar DocumentParser e Parsear Documentos
    try:
        if not os.path.exists(settings.METADATA_TSV_PATH):
            raise FileNotFoundError(f"Arquivo TSV de metadados não encontrado em: {settings.METADATA_TSV_PATH}")
        if not os.path.isdir(settings.SOURCE_DOCS_BASE_DIR):
            raise NotADirectoryError(
                f"Diretório base dos documentos fonte não é válido: {settings.SOURCE_DOCS_BASE_DIR}")

        logger.info(
            f"Inicializando DocumentParser com TSV: '{settings.METADATA_TSV_PATH}' e Base de Documentos: '{settings.SOURCE_DOCS_BASE_DIR}'")

        # AQUI ESTÁ A INSTANCIAÇÃO CORRETA
        doc_parser_instance = DocumentParser(
            metadata_tsv_path=settings.METADATA_TSV_PATH,
            source_docs_base_path=settings.SOURCE_DOCS_BASE_DIR
        )
        parsed_docs = doc_parser_instance.parse_documents()  # Chamando o método da instância

    except FileNotFoundError as e:
        logger.error(f"FALHA CRÍTICA NA INGESTÃO (Arquivo/Diretório): {e}", exc_info=True)
        return
    except NotADirectoryError as e:
        logger.error(f"FALHA CRÍTICA NA INGESTÃO (Caminho Base): {e}", exc_info=True)
        return
    except Exception as e:
        logger.error(f"FALHA CRÍTICA NA INGESTÃO (Erro no Parser): {e}", exc_info=True)
        return

    if not parsed_docs:
        logger.info("Nenhum documento foi parseado. Encerrando o pipeline de ingestão.")
        return
    logger.info(f"Sucesso: {len(parsed_docs)} documentos parseados e preparados para indexação.")

    # 2. Inicializar ElasticsearchService
    try:
        es_service = ElasticsearchService(
            es_hosts=settings.ELASTICSEARCH_HOSTS,
            es_user=settings.ELASTICSEARCH_USER,
            es_password=settings.ELASTICSEARCH_PASSWORD
        )
    except ConnectionError as e:
        logger.error(f"FALHA CRÍTICA NA INGESTÃO (Conexão ES): {e}", exc_info=False)
        return
    except Exception as e:
        logger.error(f"FALHA CRÍTICA NA INGESTÃO (Init ES Service): {e}", exc_info=True)
        return

    if should_recreate_index:
        logger.info(f"Tentando deletar o índice '{settings.ELASTICSEARCH_INDEX_NAME}'...")
        try:
            if es_service.delete_index(settings.ELASTICSEARCH_INDEX_NAME):
                time.sleep(1)
        except Exception as e:
            logger.warning(f"Não foi possível deletar o índice '{settings.ELASTICSEARCH_INDEX_NAME}': {e}",
                           exc_info=True)

    try:
        logger.info(
            f"Tentando criar o índice '{settings.ELASTICSEARCH_INDEX_NAME}' (dimensões embedding: {settings.EMBEDDING_DIMENSIONS})...")
        es_service.create_index_if_not_exists(
            index_name=settings.ELASTICSEARCH_INDEX_NAME,
            embedding_dimensions=settings.EMBEDDING_DIMENSIONS
        )
    except Exception as e:
        logger.error(f"FALHA CRÍTICA NA INGESTÃO (Criar Índice ES): {e}", exc_info=True)
        return

    documents_for_es_bulk = []
    for doc in parsed_docs:
        doc_payload = doc.copy()
        if "content_embedding" not in doc_payload:  # Garante que o campo exista para o mapeamento
            doc_payload["content_embedding"] = None
        documents_for_es_bulk.append(doc_payload)

    try:
        logger.info(f"Iniciando indexação em lote de {len(documents_for_es_bulk)} documentos...")
        success_count, errors = es_service.bulk_index_documents(
            index_name=settings.ELASTICSEARCH_INDEX_NAME,
            documents=documents_for_es_bulk
        )
        logger.info(f"Indexação em lote finalizada. Sucesso: {success_count}. Erros: {len(errors)}.")
        if errors:
            logger.error(f"Alguns documentos não foram indexados. Verifique os logs do ElasticsearchService.")
    except Exception as e:
        logger.error(f"FALHA CRÍTICA NA INGESTÃO (Bulk Indexing): {e}", exc_info=True)
        return

    overall_end_time = time.time()
    logger.info(f"Pipeline de ingestão de dados concluído em {overall_end_time - overall_start_time:.2f} segundos.")


if __name__ == "__main__":
    cli_parser = argparse.ArgumentParser(description="Pipeline de Ingestão de Dados Jurídicos.")
    cli_parser.add_argument(
        "--recreate-index", action="store_true",
        help="Deleta e recria o índice no Elasticsearch antes da ingestão."
    )
    args = cli_parser.parse_args()
    main(should_recreate_index=args.recreate_index)