# python_backend_services/data_ingestion/metadata_enricher.py
import logging
import sqlite3
import json
from typing import Dict, Any, List, Optional
import time
import os
import sys

# Bloco de importação principal
try:
    from python_backend_services.app.core.config import settings
    from python_backend_services.data_ingestion.document_parser import DocumentParser
    from python_backend_services.app.services.glossary_service import GlossaryService
    from python_backend_services.app.services.llm_service import LLMService
except ImportError as e:
    print(
        f"metadata_enricher.py: ERRO DE IMPORTAÇÃO - {e}. Tentando imports alternativos.")
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root_dir = os.path.abspath(os.path.join(current_dir, '..'))
    if project_root_dir not in sys.path:
        sys.path.insert(0, project_root_dir)
    try:
        from app.core.config import settings
        from data_ingestion.document_parser import DocumentParser
        from app.services.glossary_service import GlossaryService
        from app.services.llm_service import LLMService
    except ImportError as e_fallback:
        print(f"metadata_enricher.py: ERRO DE IMPORTAÇÃO NO FALLBACK - {e_fallback}.")


        class MockSettingsEnricher:
            SQLITE_DB_PATH = "fallback_enriched_documents.sqlite"
            METADATA_TSV_PATH = "fallback_source_metadata.tsv"
            SOURCE_DOCS_BASE_DIR = "fallback_source_documents/"
            GLOSSARY_FILE_PATH = "fallback_global_glossary.tsv"
            BATCH_SIZE_LLM_ENRICHMENT = 1
            LOG_LEVEL = "DEBUG"


        settings = MockSettingsEnricher()

# Configuração de logging
log_level_to_use = "INFO"
if 'settings' in globals() and hasattr(settings, 'LOG_LEVEL') and isinstance(settings.LOG_LEVEL, str):
    log_level_to_use = settings.LOG_LEVEL.upper()
else:
    print(f"metadata_enricher.py: WARNING - 'settings' não disponível para config de logging. Usando INFO.")

root_logger = logging.getLogger()
for handler in root_logger.handlers[:]: root_logger.removeHandler(handler)
module_logger_for_setup = logging.getLogger(__name__)
for handler in module_logger_for_setup.handlers[:]: module_logger_for_setup.removeHandler(handler)

logging.basicConfig(level=log_level_to_use,
                    format='%(asctime)s - %(levelname)s - [%(name)s:%(lineno)d] - %(message)s',
                    force=True)

logger = logging.getLogger(__name__)


class MetadataEnricher:
    def __init__(self,
                 llm_service_instance: LLMService,
                 glossary_service_instance: GlossaryService):
        self.llm_service = llm_service_instance
        self.glossary_service = glossary_service_instance
        self.db_path = settings.SQLITE_DB_PATH
        self.force_reprocess_ids = set()
        logger.info(f"MetadataEnricher inicializado. DB: {self.db_path}")

    def _get_db_connection(self) -> sqlite3.Connection:
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            return conn
        except sqlite3.Error as e:
            logger.error(f"Erro ao conectar ao SQLite em {self.db_path}: {e}", exc_info=True)
            raise

    def _insert_initial_documents_from_parser(self, parsed_docs: List[Dict[str, Any]]):
        if not parsed_docs:
            logger.info("Nenhum documento parseado para inserir/atualizar no SQLite.")
            return

        conn = self._get_db_connection()
        cursor = conn.cursor()
        inserted_count = 0
        updated_to_pending_count = 0
        skipped_already_enriched_count = 0

        try:
            for doc_data in parsed_docs:
                doc_id_str = str(doc_data.get("document_id") or doc_data.get("file_name"))

                sub_areas_value = doc_data.get("sub_areas_of_law")
                if isinstance(sub_areas_value, list):
                    sub_areas_json = json.dumps(sub_areas_value)
                elif isinstance(sub_areas_value, str) and sub_areas_value.strip():
                    sub_areas_json = json.dumps([area.strip() for area in sub_areas_value.split(';') if area.strip()])
                else:
                    sub_areas_json = json.dumps([])

                db_doc = {
                    "document_id": doc_id_str,
                    "file_name": doc_data.get("file_name"),
                    "content_path": doc_data.get("content_path"),
                    "document_title_original": doc_data.get("document_title"),
                    "summary_original": doc_data.get("summary"),
                    "first_lines_original": doc_data.get("first_lines"),
                    "document_category_original": doc_data.get("document_category"),
                    "document_type_original": doc_data.get("document_type"),
                    "legal_action_original": doc_data.get("legal_action"),
                    "legal_domain_original": doc_data.get("legal_domain"),
                    "sub_areas_of_law_original": sub_areas_json,
                    "jurisprudence_court_original": doc_data.get("jurisprudence_court"),
                    "version_original": doc_data.get("version"),
                    "full_text_content": doc_data.get("full_text_content"),
                    "status_enrichment": 'pending'
                }

                cursor.execute("SELECT status_enrichment FROM enriched_documents WHERE document_id = ?",
                               (db_doc["document_id"],))
                existing_doc_row = cursor.fetchone()

                if existing_doc_row:
                    should_update_to_pending = False
                    if db_doc[
                        "document_id"] in self.force_reprocess_ids:  # Mesmo se force_reprocess_ids estiver vazio, não afeta
                        should_update_to_pending = True
                        logger.info(
                            f"Documento ID {db_doc['document_id']} está na lista 'force_reprocess_ids' (mesmo que vazia, essa lógica é para quando há IDs). Marcando para 'pending'.")
                    elif existing_doc_row["status_enrichment"] not in ['enriched', 'processing_llm']:
                        should_update_to_pending = True

                    if should_update_to_pending:
                        update_fields = {k: v for k, v in db_doc.items() if k != "document_id"}
                        update_fields["full_text_content"] = db_doc["full_text_content"]
                        update_fields["status_enrichment"] = 'pending'

                        set_clause_parts = [f"{key} = ?" for key in update_fields.keys()]
                        values_for_update = list(update_fields.values()) + [db_doc["document_id"]]
                        set_clause = ", ".join(set_clause_parts)

                        sql_update = f"UPDATE enriched_documents SET {set_clause}, updated_at = CURRENT_TIMESTAMP WHERE document_id = ?"
                        cursor.execute(sql_update, values_for_update)
                        updated_to_pending_count += 1
                        logger.debug(
                            f"Documento ID {db_doc['document_id']} atualizado e marcado como 'pending'. Conteúdo lido: {'Sim' if db_doc['full_text_content'] else 'Não'}")
                    else:
                        skipped_already_enriched_count += 1
                else:
                    columns = ', '.join(db_doc.keys())
                    placeholders = ', '.join(['?'] * len(db_doc))
                    sql_insert = f"INSERT INTO enriched_documents ({columns}) VALUES ({placeholders})"
                    cursor.execute(sql_insert, list(db_doc.values()))
                    inserted_count += 1
                    logger.info(
                        f"Novo Documento ID {db_doc['document_id']} inserido como 'pending'. Conteúdo lido: {'Sim' if db_doc['full_text_content'] else 'Não'}")
            conn.commit()
            logger.info(f"Concluída inserção/atualização inicial no SQLite. "
                        f"Inseridos: {inserted_count}, Atualizados/Marcados para 'pending': {updated_to_pending_count}, Pulados: {skipped_already_enriched_count}")
        except sqlite3.Error as e:
            logger.error(f"Erro SQLite durante _insert_initial_documents_from_parser: {e}", exc_info=True)
            if conn: conn.rollback()
        finally:
            if conn: conn.close()

    def _build_summary_prompt(self, full_text: str, summary_type: str = "technical") -> str:
        if summary_type == "technical":
            instruction = (
                "Você é um especialista jurídico. Analise o seguinte texto de uma petição e gere um resumo técnico conciso "
                "que capture os principais pontos jurídicos, argumentos e o pedido principal. "
                "O resumo deve ser objetivo e usar terminologia jurídica apropriada. Máximo de 3-4 frases."
            )
        elif summary_type == "non_technical":
            instruction = (
                "Você é um comunicador habilidoso. Analise o seguinte texto de uma petição e gere um resumo conciso "
                "em linguagem simples, clara e acessível para um público leigo (não técnico em direito). "
                "Evite jargões jurídicos complexos. Explique o assunto principal e o que a petição busca. Máximo de 2-3 frases."
            )
        else:
            raise ValueError("Tipo de resumo desconhecido. Use 'technical' ou 'non_technical'.")
        return f"{instruction}\n\nTEXTO DA PETIÇÃO:\n\"\"\"\n{full_text[:15000]} \n\"\"\"\n\nRESUMO SOLICITADO:"

    def enrich_batch(self, batch_docs_ids: List[str]):
        if not batch_docs_ids:
            return

        conn = None
        try:
            conn = self._get_db_connection()
            cursor = conn.cursor()

            for doc_id in batch_docs_ids:
                logger.info(f"Processando enriquecimento para document_id: {doc_id}")
                cursor.execute(
                    "UPDATE enriched_documents SET status_enrichment = 'processing_llm', updated_at = CURRENT_TIMESTAMP WHERE document_id = ?",
                    (doc_id,))
                conn.commit()

                cursor.execute("SELECT full_text_content FROM enriched_documents WHERE document_id = ?", (doc_id,))
                row = cursor.fetchone()
                if not row or not row["full_text_content"]:
                    logger.error(
                        f"Conteúdo completo (full_text_content) NÃO encontrado no DB para document_id: {doc_id} na fase de enrich_batch. Pulando enriquecimento LLM.")
                    cursor.execute(
                        "UPDATE enriched_documents SET status_enrichment = 'error_enrichment', llm_error_message = 'Conteúdo não encontrado no DB para enrich_batch', updated_at = CURRENT_TIMESTAMP WHERE document_id = ?",
                        (doc_id,))
                    conn.commit()
                    continue

                full_text = row["full_text_content"]
                summary1_llm, summary2_llm = None, None
                try:
                    prompt_s1 = self._build_summary_prompt(full_text, summary_type="technical")
                    summary1_llm = self.llm_service.generate_text(prompt_s1)
                    time.sleep(0.5)
                    prompt_s2 = self._build_summary_prompt(full_text, summary_type="non_technical")
                    summary2_llm = self.llm_service.generate_text(prompt_s2)
                    time.sleep(0.5)
                except Exception as e_llm:
                    logger.error(f"Erro durante chamada ao LLM para document_id {doc_id}: {e_llm}", exc_info=True)
                    cursor.execute(
                        "UPDATE enriched_documents SET status_enrichment = 'error_enrichment', llm_error_message = ?, updated_at = CURRENT_TIMESTAMP WHERE document_id = ?",
                        (f"Erro LLM: {str(e_llm)[:450]}", doc_id))
                    conn.commit()
                    continue

                document_terms_found = self.glossary_service.find_terms_in_text(full_text, include_details=False)
                document_specific_terms_json = json.dumps(document_terms_found) if document_terms_found else json.dumps(
                    [])

                update_data = {
                    "summary1_llm": summary1_llm, "summary2_llm": summary2_llm,
                    "document_specific_terms": document_specific_terms_json,
                    "status_enrichment": 'enriched', "llm_error_message": None
                }
                set_clause = ", ".join([f"{key} = ?" for key in update_data.keys()])
                values = list(update_data.values()) + [doc_id]
                cursor.execute(
                    f"UPDATE enriched_documents SET {set_clause}, updated_at = CURRENT_TIMESTAMP WHERE document_id = ?",
                    values)
                conn.commit()
                logger.info(f"Documento {doc_id} enriquecido com sucesso.")
        except sqlite3.Error as e_sqlite_batch:
            logger.error(f"Erro SQLite durante enrich_batch: {e_sqlite_batch}", exc_info=True)
        except Exception as e_batch_general:
            logger.error(
                f"Erro geral inesperado durante enrich_batch (doc_id {doc_id if 'doc_id' in locals() else 'desconhecido'}): {e_batch_general}",
                exc_info=True)
        finally:
            if conn: conn.close()

    def run_enrichment_pipeline(self, num_docs_to_process: Optional[int] = None,
                                force_reprocess_errors: bool = False,
                                specific_ids: Optional[List[str]] = None):  # specific_ids ainda é um parâmetro
        logger.info("Iniciando pipeline de enriquecimento de metadados...")

        doc_parser = DocumentParser(
            metadata_tsv_path=settings.METADATA_TSV_PATH,  # Usará o TSV configurado em settings
            source_docs_base_path=settings.SOURCE_DOCS_BASE_DIR
        )

        # Se specific_ids forem passados, eles serão usados para forçar o reprocessamento
        # na etapa _insert_initial_documents_from_parser.
        self.force_reprocess_ids = set(str(sid) for sid in specific_ids) if specific_ids else set()
        logger.info(
            f"IDs para forçar reprocessamento (se já existirem e se specific_ids for usado na chamada): {self.force_reprocess_ids}")

        parsed_docs = doc_parser.parse_documents()
        self._insert_initial_documents_from_parser(parsed_docs)

        if hasattr(self, 'force_reprocess_ids'):
            del self.force_reprocess_ids

        # A seleção de docs_to_enrich_ids agora SEMPRE consultará o banco de dados
        # pelos status 'pending' ou 'error_enrichment' (se force_reprocess_errors=True).
        # A lógica de specific_ids agora influencia o _insert_initial_documents_from_parser
        # para marcar esses IDs como 'pending' se eles já existirem, para que sejam pegos aqui.
        docs_to_enrich_ids_final_list = []
        conn_fetch = None
        try:
            conn_fetch = self._get_db_connection()
            with conn_fetch:
                cursor = conn_fetch.cursor()
                query_statuses = ['pending']
                if force_reprocess_errors:  # Isso pegará os specific_ids (se marcados como pending) e outros erros
                    query_statuses.append('error_enrichment')

                placeholders = ','.join('?' * len(query_statuses))
                sql_query = f"SELECT document_id FROM enriched_documents WHERE status_enrichment IN ({placeholders}) ORDER BY created_at ASC"

                if num_docs_to_process is not None and num_docs_to_process > 0 and not specific_ids:  # Limita apenas se não for specific_ids
                    sql_query += f" LIMIT {num_docs_to_process}"

                cursor.execute(sql_query, query_statuses)
                docs_to_enrich_ids_final_list = [row["document_id"] for row in cursor.fetchall()]
        except sqlite3.Error as e_fetch_all:
            logger.error(f"Erro ao buscar todos os documentos para enriquecimento: {e_fetch_all}")
        finally:
            if conn_fetch: conn_fetch.close()

        # Se specific_ids foi fornecido, filtramos a lista para conter apenas esses IDs
        # (assumindo que eles foram corretamente marcados como 'pending' ou 'error_enrichment' antes)
        if specific_ids:
            specific_ids_set = set(str(sid) for sid in specific_ids)
            original_count = len(docs_to_enrich_ids_final_list)
            docs_to_enrich_ids_final_list = [doc_id for doc_id in docs_to_enrich_ids_final_list if
                                             doc_id in specific_ids_set]
            logger.info(
                f"Filtrando para specific_ids: {specific_ids}. De {original_count} documentos pendentes/erro, {len(docs_to_enrich_ids_final_list)} correspondem aos IDs especificados e estão pendentes/erro.")

        if not docs_to_enrich_ids_final_list:
            logger.info("Nenhum documento encontrado para enriquecimento (seja específico, pendente ou com erro).")
            return

        logger.info(
            f"Encontrados {len(docs_to_enrich_ids_final_list)} documentos para enriquecimento: {docs_to_enrich_ids_final_list}")

        batch_size = settings.BATCH_SIZE_LLM_ENRICHMENT  # Que agora deve ser 1 se config.py foi atualizado para teste
        for i in range(0, len(docs_to_enrich_ids_final_list), batch_size):
            batch_ids = docs_to_enrich_ids_final_list[i:i + batch_size]
            logger.info(
                f"Processando lote {i // batch_size + 1}/{(len(docs_to_enrich_ids_final_list) + batch_size - 1) // batch_size} com {len(batch_ids)} documentos: {batch_ids}")
            self.enrich_batch(batch_ids)
            logger.info(f"Lote concluído. Pausando por 3 segundos antes do próximo lote...")
            time.sleep(3)

        logger.info("Pipeline de enriquecimento de metadados concluído.")


if __name__ == '__main__':
    logger.info("--- Executando MetadataEnricher Standalone ---")

    gs_instance = None
    llm_instance = None
    enricher = None
    try:
        glossary_path_main = settings.GLOSSARY_FILE_PATH
        gs_instance = GlossaryService(glossary_tsv_path=glossary_path_main)
        if not gs_instance.glossary_data:
            logger.warning(f"GlossaryService não carregou dados de '{glossary_path_main}'. Verifique o arquivo.")

        llm_instance = LLMService(glossary_service=gs_instance)
        logger.info("Serviços GlossaryService e LLMService instanciados para teste.")
    except Exception as e_service_init:
        logger.critical(f"Falha ao instanciar serviços para MetadataEnricher: {e_service_init}", exc_info=True)
        sys.exit(1)

    if gs_instance and llm_instance:
        enricher = MetadataEnricher(
            llm_service_instance=llm_instance,
            glossary_service_instance=gs_instance
        )

        # REVERTIDO PARA O COMPORTAMENTO PADRÃO:
        # Processa todos os documentos que estiverem como 'pending' ou 'error_enrichment' (se force_reprocess_errors=True)
        # com base no arquivo METADATA_TSV_PATH definido em config.py (que deve ser o seu source_metadata.tsv principal).
        logger.info(
            f"--- INICIANDO PIPELINE DE ENRIQUECIMENTO COM BASE NO METADATA_TSV_PATH: {settings.METADATA_TSV_PATH} ---")
        # Se você quiser forçar o reprocessamento de erros específicos que já estão no banco,
        # e não apenas os novos/pendentes do TSV, mantenha force_reprocess_errors=True.
        # Se quiser processar apenas o que o TSV indicar como novo/pendente, use force_reprocess_errors=False.
        enricher.run_enrichment_pipeline(force_reprocess_errors=True)

    else:
        logger.error("Não foi possível criar instâncias do LLMService ou GlossaryService. Encerrando teste.")

    logger.info("--- Execução Standalone do MetadataEnricher Concluída ---")