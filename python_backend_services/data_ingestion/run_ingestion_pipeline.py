# python_backend_services/data_ingestion/run_ingestion_pipeline.py
import logging
import time
import argparse
import sqlite3
import json
from typing import List, Dict, Any, Optional

try:
    from python_backend_services.app.core.config import settings # Usado para caminhos e configurações de ES [cite: 8]
    from python_backend_services.data_ingestion.indexer_service import ElasticsearchService
except ImportError as e:
    print(
        f"INFO (run_ingestion_pipeline.py): Falha no import principal ({e}). Tentando imports relativos/ajuste de path...")
    import sys
    import os
    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    project_module_root = os.path.abspath(os.path.join(current_script_dir, '..'))
    if project_module_root not in sys.path:
        sys.path.insert(0, project_module_root)
        print(f"INFO (run_ingestion_pipeline.py): Adicionado '{project_module_root}' ao sys.path.")
    try:
        from app.core.config import settings
        from data_ingestion.indexer_service import ElasticsearchService
        print("INFO (run_ingestion_pipeline.py): Imports de fallback bem-sucedidos.")
    except ImportError as e_fallback:
        print(
            f"ERRO CRÍTICO (run_ingestion_pipeline.py): Falha nos imports de fallback também: {e_fallback}. Verifique a estrutura do projeto e o PYTHONPATH.")
        class MockSettingsPipeline:
            LOG_LEVEL = "ERROR"; SQLITE_DB_PATH = "error.sqlite"; ELASTICSEARCH_HOSTS = []; ELASTICSEARCH_INDEX_NAME = "error_index"; EMBEDDING_DIMENSIONS = 1
        settings = MockSettingsPipeline()

log_level_to_use = "INFO"
if 'settings' in globals() and hasattr(settings, 'LOG_LEVEL'):
    log_level_to_use = settings.LOG_LEVEL.upper()
if not logging.getLogger().handlers or not logging.getLogger("RunSQLiteToESIngestion").handlers:
    logging.basicConfig(level=log_level_to_use,
                        format='%(asctime)s - %(levelname)s - [%(name)s:%(lineno)d] - %(message)s',
                        force=True)

logger = logging.getLogger("RunSQLiteToESIngestion")


def fetch_enriched_documents_from_sqlite(limit: Optional[int] = None, offset: int = 0) -> List[Dict[str, Any]]: # [cite: 8]
    docs_for_es = []
    conn = None
    try:
        logger.debug(f"Conectando ao SQLite em: {settings.SQLITE_DB_PATH}") # SQLITE_DB_PATH de config.py [cite: 8]
        conn = sqlite3.connect(settings.SQLITE_DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        query_params = []
        sql_query = "SELECT * FROM enriched_documents WHERE status_enrichment = 'enriched' ORDER BY document_id" # Busca docs enriquecidos [cite: 8]

        if limit is not None and limit > 0:
            sql_query += " LIMIT ?"
            query_params.append(limit)
        if offset > 0:
            if not (limit is not None and limit > 0):
                sql_query += " LIMIT -1"
            sql_query += " OFFSET ?"
            query_params.append(offset)
        query_params_tuple = tuple(query_params)

        logger.info(f"Buscando documentos enriquecidos do SQLite. Query: {sql_query} com params {query_params_tuple}")
        cursor.execute(sql_query, query_params_tuple)
        rows = cursor.fetchall()
        logger.info(
            f"Encontrados {len(rows)} documentos enriquecidos no SQLite para o lote atual (offset={offset}, limit={limit}).")

        for row_sqlite in rows:
            doc_dict = dict(row_sqlite)
            # "document_specific_terms" foi populado pelo MetadataEnricher usando GlossaryService.
            # Contém os "termo_juridico" (Coluna B) do glossário.
            # "sub_areas_of_law_llm" e "legal_domain_llm" (análogos aos campos E e F) também são processados.
            for json_field in ["sub_areas_of_law_original", "sub_areas_of_law_llm", "document_specific_terms", # [cite: 8]
                               "extracted_tags"]:
                field_value = doc_dict.get(json_field)
                if field_value and isinstance(field_value, str):
                    try:
                        parsed_value = json.loads(field_value)
                        if isinstance(parsed_value, list):
                            doc_dict[json_field] = [str(item) for item in parsed_value if item is not None]
                        elif isinstance(parsed_value, str):
                            doc_dict[json_field] = [parsed_value]
                        else:
                            logger.warning(
                                f"Campo JSON '{json_field}' para doc_id {doc_dict.get('document_id')} não é lista nem string após parse. Valor: {parsed_value}. Usando lista vazia [].")
                            doc_dict[json_field] = []
                    except json.JSONDecodeError:
                        logger.warning(
                            f"Falha ao parsear campo JSON '{json_field}' para doc_id {doc_dict.get('document_id')}. Valor: '{field_value}'. Tratando como lista de string única ou vazia.")
                        doc_dict[json_field] = [field_value] if field_value.strip() else []
                elif field_value is None:
                    doc_dict[json_field] = []
                elif isinstance(field_value, list):
                    doc_dict[json_field] = [str(item) for item in field_value if item is not None]
                elif field_value is not None:
                    logger.warning(
                        f"Campo '{json_field}' para doc_id {doc_dict.get('document_id')} não é string, nem lista, nem None. Valor: {field_value} (Tipo: {type(field_value)}). Convertendo para lista vazia [].")
                    doc_dict[json_field] = []

            for date_field in ["created_at", "updated_at"]:
                if date_field in doc_dict and doc_dict[date_field] is not None:
                    doc_dict[date_field] = str(doc_dict[date_field])

            if "content_embedding" not in doc_dict:
                doc_dict["content_embedding"] = None
            docs_for_es.append(doc_dict)
        return docs_for_es
    except sqlite3.Error as e:
        logger.error(f"Erro ao buscar documentos do SQLite: {e}", exc_info=True)
        return []
    finally:
        if conn:
            conn.close()
            logger.debug("Conexão SQLite fechada em fetch_enriched_documents_from_sqlite.")


def main_sqlite_to_es(should_recreate_index: bool, batch_size_es: int = 500): # [cite: 8]
    logger.info(
        f"Pipeline de ingestão SQLite para Elasticsearch iniciado. Recriar índice: {should_recreate_index}, Tamanho do Lote ES: {batch_size_es}")
    start_time_pipeline = time.time()
    es_service = None
    try:
        es_service = ElasticsearchService( # Usa configurações de ES de config.py [cite: 8]
            es_hosts=settings.ELASTICSEARCH_HOSTS,
            es_user=settings.ELASTICSEARCH_USER,
            es_password=settings.ELASTICSEARCH_PASSWORD
        )
    except Exception as e_es_init:
        logger.critical(f"Falha crítica ao inicializar ElasticsearchService: {e_es_init}", exc_info=True)
        return

    target_index_name = settings.ELASTICSEARCH_INDEX_NAME # De config.py [cite: 8]
    try:
        if should_recreate_index:
            logger.info(f"Opção de recriar índice selecionada. Deletando índice '{target_index_name}' se existir...")
            es_service.delete_index(target_index_name)
            time.sleep(1)
        logger.info(
            f"Garantindo que o índice '{target_index_name}' exista com o mapeamento correto (embedding_dims: {settings.EMBEDDING_DIMENSIONS})...") # EMBEDDING_DIMENSIONS de config.py [cite: 8]
        es_service.create_index_if_not_exists(
            index_name=target_index_name,
            embedding_dimensions=settings.EMBEDDING_DIMENSIONS
        )
    except Exception as e_create_idx:
        logger.critical(
            f"Falha crítica durante a preparação do índice Elasticsearch '{target_index_name}': {e_create_idx}",
            exc_info=True)
        return

    offset = 0
    total_docs_indexed_successfully = 0
    total_docs_with_errors = 0
    while True:
        logger.info(f"Buscando próximo lote de documentos do SQLite (offset={offset}, batch_size={batch_size_es})...")
        documents_to_index = fetch_enriched_documents_from_sqlite(limit=batch_size_es, offset=offset) # Busca dados que podem conter termos do glossário [cite: 8]

        if not documents_to_index:
            logger.info("Nenhum documento adicional encontrado no SQLite com status 'enriched' para indexar.")
            break

        logger.info(
            f"Preparando para indexar {len(documents_to_index)} documentos deste lote no índice '{target_index_name}'...")
        try:
            s_count, errs = es_service.bulk_index_documents(target_index_name, documents_to_index) # Indexa os dados no ES [cite: 8]
            total_docs_indexed_successfully += s_count
            num_errors_in_batch = len(errs) if errs else 0
            total_docs_with_errors += num_errors_in_batch
            logger.info(
                f"Lote (offset={offset}) processado. Documentos enviados ao ES: {len(documents_to_index)}. Sucesso na indexação ES: {s_count}. Erros no ES: {num_errors_in_batch}.")
            if errs and num_errors_in_batch > 0:
                logger.warning(
                    f"Erros na indexação deste lote. O ElasticsearchService já logou os primeiros 5 erros detalhados.")
        except Exception as e_bulk_main:
            logger.error(f"Falha crítica durante o bulk indexing do lote (offset={offset}): {e_bulk_main}",
                         exc_info=True)
            logger.critical("Parando pipeline devido a erro crítico no bulk indexing.")
            break
        offset += len(documents_to_index)
        logger.info(f"Avançando offset para {offset}. Pausando brevemente...")
        time.sleep(0.2)

    logger.info(f"Pipeline SQLite para Elasticsearch concluído em {time.time() - start_time_pipeline:.2f}s.")
    logger.info(f"Total de documentos indexados com sucesso no Elasticsearch: {total_docs_indexed_successfully}")
    logger.info(
        f"Total de documentos que resultaram em erro durante a indexação (reportados pelo ES): {total_docs_with_errors}")


if __name__ == "__main__":
    cli_parser = argparse.ArgumentParser(description="Pipeline de ingestão de dados do SQLite para Elasticsearch.")
    cli_parser.add_argument(
        "--recreate-index",
        action="store_true",
        help="Deleta e recria o índice do Elasticsearch antes da ingestão. Use com CUIDADO."
    )
    cli_parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Número de documentos a serem processados por lote do SQLite para o ES."
    )
    args = cli_parser.parse_args()
    main_sqlite_to_es(should_recreate_index=args.recreate_index, batch_size_es=args.batch_size) # [cite: 8]