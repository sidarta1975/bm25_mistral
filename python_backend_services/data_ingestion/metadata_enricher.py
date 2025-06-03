# python_backend_services/data_ingestion/metadata_enricher.py
import logging
import sqlite3
import json
from typing import Dict, Any, List, Optional
import time
import os
import sys
import gspread

# Bloco de importação principal
try:
    from python_backend_services.app.core.config import settings
    from python_backend_services.app.services.glossary_service import GlossaryService
    from python_backend_services.app.services.llm_service import LLMService
    # Importar ElasticsearchService para a nova etapa de indexação
    from python_backend_services.data_ingestion.indexer_service import ElasticsearchService
except ImportError as e:
    print(
        f"metadata_enricher.py: ERRO DE IMPORTAÇÃO - {e}. Tentando imports alternativos.")
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root_dir = os.path.abspath(os.path.join(current_dir, '..'))
    if project_root_dir not in sys.path:
        sys.path.insert(0, project_root_dir)
    try:
        from app.core.config import settings
        from app.services.glossary_service import GlossaryService
        from app.services.llm_service import LLMService
        from data_ingestion.indexer_service import ElasticsearchService  # Fallback import
    except ImportError as e_fallback:
        print(f"metadata_enricher.py: ERRO DE IMPORTAÇÃO NO FALLBACK - {e_fallback}.")


        class MockSettingsEnricher:
            SQLITE_DB_PATH = "fallback_enriched_documents.sqlite"
            SOURCE_DOCS_BASE_DIR = "fallback_source_documents/"
            GCP_SERVICE_ACCOUNT_KEY_PATH = "fallback_credentials.json"
            SOURCE_METADATA_GSHEET_ID = "mock_metadata_sheet_id"
            SOURCE_METADATA_GSHEET_TAB_NAME = "Página1"
            GLOBAL_GLOSSARY_GSHEET_ID = "mock_glossary_sheet_id"
            GLOBAL_GLOSSARY_GSHEET_TAB_NAME = "Sheet1"
            ELASTICSEARCH_HOSTS = ["http://localhost:9200"]  # Mock ES
            ELASTICSEARCH_INDEX_NAME = "fallback_test_index"  # Mock ES
            ELASTICSEARCH_USER = None
            ELASTICSEARCH_PASSWORD = None
            BATCH_SIZE_LLM_ENRICHMENT = 1
            LOG_LEVEL = "DEBUG"


        settings = MockSettingsEnricher()
        # Mock ElasticsearchService se não puder ser importado
        if 'ElasticsearchService' not in globals():
            class MockElasticsearchService:
                def __init__(self, *args, **kwargs): logger.warning("Usando MockElasticsearchService.")

                def index_document(self, *args, **kwargs): logger.warning("MockES: index_document chamado.")

                def create_index_if_not_exists(self, *args, **kwargs): logger.warning(
                    "MockES: create_index_if_not_exists chamado.")


            ElasticsearchService = MockElasticsearchService

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


class DocumentParserGS:
    def __init__(self, service_account_path: str, gsheet_id: str, gsheet_tab_name: str, source_docs_base_path: str):
        self.service_account_path = service_account_path
        self.gsheet_id = gsheet_id
        self.gsheet_tab_name = gsheet_tab_name
        self.source_docs_base_path = source_docs_base_path
        logger.info(f"DocumentParserGS inicializado para GSheet ID: {gsheet_id}, Aba: {gsheet_tab_name}")

    def parse_documents(self) -> List[Dict[str, Any]]:
        logger.info("Iniciando parsing de documentos a partir do Google Sheets (metadados).")
        parsed_docs = []
        try:
            gc = gspread.service_account(filename=self.service_account_path)
            spreadsheet = gc.open_by_key(self.gsheet_id)
            worksheet = spreadsheet.worksheet(self.gsheet_tab_name)
            gsheet_data = worksheet.get_all_records()
            logger.info(f"Lidos {len(gsheet_data)} registros da planilha de metadados.")

            for i, row_dict in enumerate(gsheet_data):
                doc_data = {
                    # CORREÇÃO: Converte todos os gets para string ANTES de .strip()
                    "document_id": str(row_dict.get("document_id", f"auto_id_{i + 2}")).strip(),
                    # i+2 para corresponder a linha da planilha se header é 1
                    "file_name": str(row_dict.get("file_name", "")).strip(),
                    "document_title": str(row_dict.get("name_text", "")).strip(),
                    "legal_domain": str(row_dict.get("legal_domain", "")).strip(),
                    "sub_areas_of_law": str(row_dict.get("sub_areas_of_law", "")).strip(),
                    "summary": str(row_dict.get("summary", "")).strip(),
                    "first_lines": str(row_dict.get("primeiras_20_linhas", "")).strip(),
                    "document_category_original": str(row_dict.get("document_category_original", "")).strip(),
                    "document_type_original": str(row_dict.get("document_type_original", "")).strip(),
                    "legal_action_original": str(row_dict.get("legal_action_original", "")).strip(),
                    "jurisprudence_court_original": str(row_dict.get("jurisprudence_court_original", "")).strip(),
                    # Adicionado
                    "version_original": str(row_dict.get("version_original", "")).strip()  # Adicionado
                }

                if not doc_data["document_id"]:  # Pula linhas completamente vazias ou sem ID
                    logger.warning(f"Registro na linha da planilha {i + 2} sem 'document_id' válido. Pulando.")
                    continue

                if doc_data.get("file_name"):
                    content_path = os.path.join(self.source_docs_base_path, doc_data["file_name"])
                    doc_data["content_path"] = content_path
                    try:
                        with open(content_path, 'r', encoding='utf-8') as f:
                            doc_data["full_text_content"] = f.read()
                        logger.debug(f"Conteúdo lido para {doc_data['file_name']} (ID: {doc_data['document_id']})")
                    except FileNotFoundError:
                        logger.error(
                            f"Arquivo de texto não encontrado: {content_path} para document_id: {doc_data['document_id']}")
                        doc_data["full_text_content"] = None
                    except Exception as e:
                        logger.error(f"Erro ao ler o arquivo {content_path} (ID: {doc_data['document_id']}): {e}")
                        doc_data["full_text_content"] = None
                else:
                    logger.warning(
                        f"Document_id {doc_data['document_id']} não possui 'file_name' associado na planilha.")
                    doc_data["full_text_content"] = None
                    doc_data["content_path"] = None

                parsed_docs.append(doc_data)

            logger.info(
                f"Parsing do Google Sheets (metadados) concluído. {len(parsed_docs)} documentos válidos processados.")
            return parsed_docs

        except FileNotFoundError:
            logger.error(f"ERRO CRÍTICO: Arquivo de credenciais '{self.service_account_path}' não encontrado.",
                         exc_info=True)
        except gspread.exceptions.SpreadsheetNotFound:
            logger.error(f"ERRO CRÍTICO: Planilha de METADADOS com ID '{self.gsheet_id}' não encontrada.",
                         exc_info=True)
        except gspread.exceptions.WorksheetNotFound:
            logger.error(
                f"ERRO CRÍTICO: Aba de METADADOS '{self.gsheet_tab_name}' não encontrada na planilha ID '{self.gsheet_id}'.",
                exc_info=True)
        except Exception as e:
            logger.error(f"Erro geral ao acessar Google Sheets para metadados: {e}", exc_info=True)
        return []


class GlossaryServiceGS(GlossaryService):
    def __init__(self, service_account_path: str, gsheet_id: str, gsheet_tab_name: str, *args, **kwargs):
        self.service_account_path = service_account_path
        self.gsheet_id = gsheet_id
        self.gsheet_tab_name = gsheet_tab_name
        self.glossary_data = self._load_glossary_data_from_gsheet()
        # A mensagem de log original está boa, mas pode ser ajustada se _load_glossary_data_from_gsheet falhar e retornar {}
        if self.glossary_data:
            logger.info(
                f"GlossaryServiceGS inicializado. {len(self.glossary_data)} termos carregados do GSheet ID: {gsheet_id}, Aba: {gsheet_tab_name}")
        else:
            logger.warning(
                f"GlossaryServiceGS inicializado, mas nenhum termo foi carregado. Verifique a planilha do glossário e as permissões.")

    def _load_glossary_data_from_gsheet(self) -> Dict[str, Dict[str, str]]:
        logger.info(f"Carregando dados do glossário do GSheet ID: {self.gsheet_id}, Aba: {self.gsheet_tab_name}")
        glossary_data_map = {}
        try:
            gc = gspread.service_account(filename=self.service_account_path)
            spreadsheet = gc.open_by_key(self.gsheet_id)
            worksheet = spreadsheet.worksheet(self.gsheet_tab_name)
            glossary_entries = worksheet.get_all_records()
            logger.info(f"Lidos {len(glossary_entries)} registros da planilha de glossário.")

            for entry in glossary_entries:
                # CORREÇÃO: Garantir que 'termo_juridico' seja string antes de strip e lower
                term = str(entry.get('termo_juridico', '')).strip().lower()
                if term:
                    glossary_data_map[term] = {
                        # CORREÇÃO: Garantir que todos os valores sejam strings
                        "summary_tec": str(entry.get('summary_tec', '')),
                        "summary_public": str(entry.get('summary_public', '')),
                        "sub_areas_of_law": str(entry.get('sub_areas_of_law', '')),
                        "legal_domain": str(entry.get('legal_domain', ''))
                    }
            logger.info(f"Glossário carregado do Google Sheets. {len(glossary_data_map)} termos processados.")
            return glossary_data_map
        except FileNotFoundError:
            logger.error(
                f"ERRO CRÍTICO ao carregar glossário: Arquivo de credenciais '{self.service_account_path}' não encontrado.",
                exc_info=True)
        except gspread.exceptions.SpreadsheetNotFound:
            logger.error(f"ERRO CRÍTICO: Planilha de GLOSSÁRIO com ID '{self.gsheet_id}' não encontrada.",
                         exc_info=True)
        except gspread.exceptions.WorksheetNotFound:
            logger.error(
                f"ERRO CRÍTICO: Aba de GLOSSÁRIO '{self.gsheet_tab_name}' não encontrada na planilha ID '{self.gsheet_id}'.",
                exc_info=True)
        except Exception as e:
            logger.error(f"Erro ao carregar o glossário do Google Sheets: {e}", exc_info=True)
        return {}


class MetadataEnricher:
    def __init__(self,
                 llm_service_instance: LLMService,
                 glossary_service_instance: GlossaryService,
                 es_service_instance: ElasticsearchService):  # Adicionado es_service_instance
        self.llm_service = llm_service_instance
        self.glossary_service = glossary_service_instance
        self.es_service = es_service_instance  # Adicionado
        self.db_path = settings.SQLITE_DB_PATH
        self.force_reprocess_ids = set()
        logger.info(f"MetadataEnricher inicializado. DB: {self.db_path}. ES Index: {settings.ELASTICSEARCH_INDEX_NAME}")

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
            logger.info("Nenhum documento parseado (do Google Sheets) para inserir/atualizar no SQLite.")
            return

        conn = self._get_db_connection()
        cursor = conn.cursor()
        inserted_count = 0
        updated_to_pending_count = 0
        skipped_already_enriched_count = 0

        try:
            for doc_data in parsed_docs:
                doc_id_str = str(doc_data.get("document_id"))  # Já deve ser string do parser

                sub_areas_value = doc_data.get("sub_areas_of_law")  # Já deve ser string do parser
                if isinstance(sub_areas_value, str) and sub_areas_value.strip():
                    sub_areas_json = json.dumps([area.strip() for area in sub_areas_value.split(';') if area.strip()])
                else:
                    sub_areas_json = json.dumps([])  # Default para lista vazia se vazio ou não string

                db_doc = {
                    "document_id": doc_id_str,
                    "file_name": doc_data.get("file_name"),
                    "content_path": doc_data.get("content_path"),
                    "document_title_original": doc_data.get("document_title"),
                    "summary_original": doc_data.get("summary"),
                    "first_lines_original": doc_data.get("first_lines"),
                    "document_category_original": doc_data.get("document_category_original"),
                    "document_type_original": doc_data.get("document_type_original"),
                    "legal_action_original": doc_data.get("legal_action_original"),
                    "legal_domain_original": doc_data.get("legal_domain"),
                    "sub_areas_of_law_original": sub_areas_json,
                    "jurisprudence_court_original": doc_data.get("jurisprudence_court_original"),
                    "version_original": doc_data.get("version_original"),
                    "full_text_content": doc_data.get("full_text_content"),
                    "status_enrichment": 'pending'  # Default para novos ou atualizados
                }

                cursor.execute(
                    "SELECT status_enrichment, llm_error_message FROM enriched_documents WHERE document_id = ?",
                    (doc_id_str,))
                existing_doc_row = cursor.fetchone()

                if existing_doc_row:
                    should_update_and_set_pending = False
                    # Força reprocessamento se ID estiver na lista OU se o status não for 'enriched' E não houver erro LLM persistente
                    # A ideia é não reprocessar infinitamente algo com erro LLM, a menos que explicitamente forçado.
                    if doc_id_str in self.force_reprocess_ids:
                        should_update_and_set_pending = True
                        logger.info(
                            f"Documento ID {doc_id_str} está na lista 'force_reprocess_ids'. Marcando para 'pending'.")
                    elif existing_doc_row[
                        "status_enrichment"] == 'error_enrichment' and not force_reprocess_errors:  # Se force_reprocess_errors é False (padrão)
                        logger.info(
                            f"Documento ID {doc_id_str} com 'error_enrichment' será mantido assim, a menos que 'force_reprocess_errors' seja True na chamada do pipeline.")
                        # Não altera para pending, mantém o erro.
                    elif existing_doc_row["status_enrichment"] not in ['enriched', 'processing_llm']:
                        # Se for 'pending' (de uma execução anterior interrompida) ou um status desconhecido, ou erro e force_reprocess_errors=True
                        should_update_and_set_pending = True

                    # Se o conteúdo do arquivo mudou (ex: full_text_content é diferente do que está no DB),
                    # idealmente você compararia um hash ou data de modificação.
                    # Por simplicidade, aqui estamos assumindo que se o parser fornece full_text_content,
                    # ele deve ser usado para atualizar.
                    # Esta lógica pode ser refinada para ser mais seletiva sobre quando resetar para 'pending'.

                    if should_update_and_set_pending:
                        # Atualiza todos os campos parseados e reseta o status para 'pending'
                        # Isso garante que dados da GSheet (como título, resumo original) sejam atualizados no SQLite
                        update_fields = {k: v for k, v in db_doc.items() if
                                         k not in ["document_id", "status_enrichment"]}
                        update_fields["status_enrichment"] = 'pending'
                        update_fields["llm_error_message"] = None  # Limpa erro anterior ao reprocessar

                        set_clause_parts = [f"{key} = ?" for key in update_fields.keys()]
                        values_for_update = list(update_fields.values()) + [doc_id_str]
                        set_clause = ", ".join(set_clause_parts)

                        sql_update = f"UPDATE enriched_documents SET {set_clause}, updated_at = CURRENT_TIMESTAMP WHERE document_id = ?"
                        cursor.execute(sql_update, values_for_update)
                        updated_to_pending_count += 1
                        logger.info(
                            f"Documento ID {doc_id_str} existente atualizado com dados da GSheet e marcado como 'pending'.")
                    else:
                        skipped_already_enriched_count += 1
                        logger.debug(
                            f"Documento ID {doc_id_str} já existe e está 'enriched' ou 'processing_llm' (ou 'error_enrichment' sem force_reprocess). Pulando atualização para 'pending'.")
                else:  # Documento não existe no SQLite, insere novo
                    columns = ', '.join(db_doc.keys())
                    placeholders = ', '.join(['?'] * len(db_doc))
                    sql_insert = f"INSERT INTO enriched_documents ({columns}) VALUES ({placeholders})"
                    cursor.execute(sql_insert, list(db_doc.values()))
                    inserted_count += 1
                    logger.info(f"Novo Documento ID {doc_id_str} (da GSheet) inserido no SQLite como 'pending'.")

            conn.commit()
            logger.info(f"Concluída inserção/atualização inicial no SQLite (fonte: Google Sheets). "
                        f"Novos Inseridos: {inserted_count}, Atualizados para 'pending': {updated_to_pending_count}, Pulados (já OK): {skipped_already_enriched_count}")

        except sqlite3.Error as e:
            logger.error(f"Erro SQLite durante _insert_initial_documents_from_parser: {e}", exc_info=True)
            if conn: conn.rollback()
        finally:
            if conn: conn.close()

    def _build_summary_prompt(self, full_text: str, summary_type: str = "technical") -> str:
        if not full_text or not full_text.strip():  # Checagem adicional
            logger.warning("Tentativa de gerar resumo para texto vazio ou nulo.")
            return ""  # Retorna prompt vazio para evitar erro no LLM, ou pode levantar exceção
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
        # Limita o tamanho do full_text para evitar prompts excessivamente longos para o LLM
        max_prompt_text_len = 15000
        return f"{instruction}\n\nTEXTO DA PETIÇÃO:\n\"\"\"\n{full_text[:max_prompt_text_len]} \n\"\"\"\n\nRESUMO SOLICITADO:"

    def enrich_batch(self, batch_docs_ids: List[str]):
        if not batch_docs_ids:
            return

        conn = None
        try:
            conn = self._get_db_connection()
            cursor = conn.cursor()

            for doc_id in batch_docs_ids:
                logger.info(f"Processando enriquecimento para document_id: {doc_id}")
                # Primeiro, busca o full_text_content para evitar processar se não houver conteúdo
                cursor.execute("SELECT full_text_content FROM enriched_documents WHERE document_id = ?", (doc_id,))
                row = cursor.fetchone()

                if not row or not row["full_text_content"]:
                    logger.error(
                        f"Conteúdo completo (full_text_content) NÃO encontrado no DB ou está VAZIO para document_id: {doc_id} na fase de enrich_batch. Pulando enriquecimento LLM.")
                    # Atualiza status para erro, mas não sobrescreve erro LLM se já houver um mais específico.
                    # Ou decide limpar erro LLM se a causa for ausência de conteúdo.
                    cursor.execute(
                        "UPDATE enriched_documents SET status_enrichment = 'error_enrichment', llm_error_message = COALESCE(llm_error_message, 'Conteúdo não encontrado no DB para enrich_batch'), updated_at = CURRENT_TIMESTAMP WHERE document_id = ?",
                        (doc_id,))
                    conn.commit()
                    continue  # Pula para o próximo doc_id no lote

                # Se tem conteúdo, marca como 'processing_llm'
                cursor.execute(
                    "UPDATE enriched_documents SET status_enrichment = 'processing_llm', llm_error_message = NULL, updated_at = CURRENT_TIMESTAMP WHERE document_id = ?",
                    (doc_id,))  # Limpa erro LLM anterior ao tentar reprocessar
                conn.commit()

                full_text = row["full_text_content"]
                summary1_llm, summary2_llm = None, None
                llm_call_successful = True
                try:
                    prompt_s1 = self._build_summary_prompt(full_text, summary_type="technical")
                    if prompt_s1: summary1_llm = self.llm_service.generate_text(prompt_s1)
                    time.sleep(0.5)
                    prompt_s2 = self._build_summary_prompt(full_text, summary_type="non_technical")
                    if prompt_s2: summary2_llm = self.llm_service.generate_text(prompt_s2)
                    time.sleep(0.5)
                except Exception as e_llm:
                    llm_call_successful = False
                    logger.error(f"Erro durante chamada ao LLM para document_id {doc_id}: {e_llm}", exc_info=True)
                    cursor.execute(
                        "UPDATE enriched_documents SET status_enrichment = 'error_enrichment', llm_error_message = ?, updated_at = CURRENT_TIMESTAMP WHERE document_id = ?",
                        (f"Erro LLM: {str(e_llm)[:450]}", doc_id))
                    conn.commit()
                    continue  # Pula para o próximo doc_id no lote

                if not llm_call_successful:  # Redundante devido ao continue acima, mas para clareza
                    continue

                document_terms_found = self.glossary_service.find_terms_in_text(full_text, include_details=False)
                document_specific_terms_json = json.dumps(document_terms_found) if document_terms_found else json.dumps(
                    [])

                update_data = {
                    "summary1_llm": summary1_llm,
                    "summary2_llm": summary2_llm,
                    "document_specific_terms": document_specific_terms_json,
                    "status_enrichment": 'enriched',  # Marcado como enriquecido
                    "llm_error_message": None  # Limpa qualquer erro anterior se o enriquecimento foi bem-sucedido
                }
                set_clause = ", ".join([f"{key} = ?" for key in update_data.keys()])
                values = list(update_data.values()) + [doc_id]
                cursor.execute(
                    f"UPDATE enriched_documents SET {set_clause}, updated_at = CURRENT_TIMESTAMP WHERE document_id = ?",
                    values)
                conn.commit()
                logger.info(f"Documento {doc_id} enriquecido com sucesso no SQLite.")
        except sqlite3.Error as e_sqlite_batch:
            logger.error(f"Erro SQLite durante enrich_batch: {e_sqlite_batch}", exc_info=True)
            # Considerar como lidar com o status dos documentos no lote se a conexão cair no meio.
        except Exception as e_batch_general:
            logger.error(
                f"Erro geral inesperado durante enrich_batch (processando IDs {batch_docs_ids}): {e_batch_general}",
                exc_info=True)
        finally:
            if conn: conn.close()

    def index_enriched_documents_to_elasticsearch(self):
        """
        Lê documentos com status 'enriched' do SQLite e os indexa no Elasticsearch.
        """
        logger.info("Iniciando a indexação de documentos enriquecidos do SQLite para o Elasticsearch...")
        conn = None
        indexed_count = 0
        failed_count = 0
        try:
            conn = self._get_db_connection()
            cursor = conn.cursor()
            # Adicionar um campo 'indexed_at_es' ou 'status_es_sync' no SQLite seria ideal
            # para evitar reindexar desnecessariamente ou para rastrear erros de sincronização.
            # Por ora, vamos pegar todos os 'enriched'.
            cursor.execute("SELECT * FROM enriched_documents WHERE status_enrichment = 'enriched'")
            enriched_docs_to_index = cursor.fetchall()

            if not enriched_docs_to_index:
                logger.info("Nenhum documento 'enriched' no SQLite para indexar no Elasticsearch.")
                return

            logger.info(
                f"Encontrados {len(enriched_docs_to_index)} documentos 'enriched' para indexar no Elasticsearch.")

            for row in enriched_docs_to_index:
                doc_dict = dict(row)
                doc_id_es = doc_dict.pop("document_id")  # Usa como _id no ES

                # Seleciona/transforma campos para o corpo do documento no Elasticsearch
                # Remova campos que não devem ir para o ES ou que são apenas para o SQLite
                doc_body_for_es = {
                    "file_name": doc_dict.get("file_name"),
                    "content_path": doc_dict.get("content_path"),
                    "document_title_original": doc_dict.get("document_title_original"),
                    "summary_original": doc_dict.get("summary_original"),
                    "first_lines_original": doc_dict.get("first_lines_original"),
                    "document_category_original": doc_dict.get("document_category_original"),
                    "document_type_original": doc_dict.get("document_type_original"),
                    "legal_action_original": doc_dict.get("legal_action_original"),
                    "legal_domain_original": doc_dict.get("legal_domain_original"),
                    "sub_areas_of_law_original": json.loads(doc_dict.get("sub_areas_of_law_original", "[]")),
                    # Deserializa
                    "jurisprudence_court_original": doc_dict.get("jurisprudence_court_original"),
                    "version_original": doc_dict.get("version_original"),
                    "full_text_content": doc_dict.get("full_text_content"),  # Importante para busca
                    "document_title_llm": doc_dict.get("document_title_llm"),
                    "summary1_llm": doc_dict.get("summary1_llm"),
                    "summary2_llm": doc_dict.get("summary2_llm"),
                    "legal_domain_llm": doc_dict.get("legal_domain_llm"),
                    "sub_areas_of_law_llm": json.loads(doc_dict.get("sub_areas_of_law_llm", "[]") or "[]"),
                    # Deserializa, com fallback para string vazia antes do json.loads
                    "document_specific_terms": json.loads(doc_dict.get("document_specific_terms", "[]") or "[]"),
                    # Deserializa
                    "updated_at": doc_dict.get("updated_at")  # Pode ser útil no ES
                }
                # Remove chaves com valor None para não enviar campos nulos desnecessariamente ao ES
                doc_body_for_es_cleaned = {k: v for k, v in doc_body_for_es.items() if v is not None}

                try:
                    # Assumindo que seu ElasticsearchService tem um método index_document
                    self.es_service.index_document(
                        index_name=settings.ELASTICSEARCH_INDEX_NAME,
                        doc_id=doc_id_es,
                        document_body=doc_body_for_es_cleaned
                    )
                    logger.info(f"Documento ID {doc_id_es} indexado com sucesso no Elasticsearch.")
                    indexed_count += 1
                    # Opcional: Atualizar status no SQLite para 'indexed_in_es'
                    # cursor.execute("UPDATE enriched_documents SET status_es_sync = 'synced', updated_at = CURRENT_TIMESTAMP WHERE document_id = ?", (doc_id_es,))
                except Exception as e_index:
                    logger.error(f"Erro ao indexar documento ID {doc_id_es} no Elasticsearch: {e_index}", exc_info=True)
                    failed_count += 1
                    # Opcional: Atualizar status no SQLite para 'error_es_sync'
                    # cursor.execute("UPDATE enriched_documents SET status_es_sync = 'error', es_sync_error_message = ?, updated_at = CURRENT_TIMESTAMP WHERE document_id = ?", (str(e_index)[:450], doc_id_es))

            if conn: conn.commit()  # Commita as atualizações de status_es_sync, se houver
            logger.info(f"Indexação para Elasticsearch concluída. Sucesso: {indexed_count}, Falhas: {failed_count}.")

        except sqlite3.Error as e_sqlite_fetch:
            logger.error(f"Erro SQLite ao buscar documentos enriquecidos para indexação ES: {e_sqlite_fetch}",
                         exc_info=True)
        except AttributeError as ae:
            if 'es_service' not in self.__dict__ or not self.es_service:
                logger.error(
                    f"ElasticsearchService (self.es_service) não foi inicializado corretamente no MetadataEnricher. {ae}",
                    exc_info=True)
            else:
                logger.error(f"Erro de Atributo durante indexação ES: {ae}", exc_info=True)
        except Exception as e_es_sync_main:
            logger.error(f"Erro geral durante a sincronização do SQLite para Elasticsearch: {e_es_sync_main}",
                         exc_info=True)
        finally:
            if conn: conn.close()

    def run_enrichment_pipeline(self, num_docs_to_process: Optional[int] = None,
                                force_reprocess_errors: bool = False,
                                specific_ids: Optional[List[str]] = None):
        logger.info("Iniciando pipeline de enriquecimento de metadados (usando Google Sheets como fonte).")

        global force_reprocess_errors_flag  # Para uso em _insert_initial_documents_from_parser
        force_reprocess_errors_flag = force_reprocess_errors

        doc_parser_gs = DocumentParserGS(
            service_account_path=settings.GCP_SERVICE_ACCOUNT_KEY_PATH,
            gsheet_id=settings.SOURCE_METADATA_GSHEET_ID,
            gsheet_tab_name=settings.SOURCE_METADATA_GSHEET_TAB_NAME,
            source_docs_base_path=settings.SOURCE_DOCS_BASE_DIR
        )

        self.force_reprocess_ids = set(
            str(sid).strip() for sid in specific_ids if sid and str(sid).strip()) if specific_ids else set()

        logger.info(
            f"IDs específicos para forçar reprocessamento (influencia _insert_initial_documents): {self.force_reprocess_ids if self.force_reprocess_ids else 'Nenhum'}")
        logger.info(
            f"Flag force_reprocess_errors (influencia _insert_initial_documents e busca no SQLite): {force_reprocess_errors}")

        parsed_docs_from_gsheet = doc_parser_gs.parse_documents()
        # _insert_initial_documents_from_parser agora respeita melhor o estado 'enriched' e 'error_enrichment'
        self._insert_initial_documents_from_parser(parsed_docs_from_gsheet)

        # Limpa o atributo após o uso em _insert_initial_documents_from_parser
        if hasattr(self, 'force_reprocess_ids'):
            del self.force_reprocess_ids

        docs_to_enrich_ids_final_list = []
        conn_fetch = None
        try:
            conn_fetch = self._get_db_connection()
            with conn_fetch:
                cursor = conn_fetch.cursor()
                query_statuses = ['pending']  # Sempre pega 'pending'
                if force_reprocess_errors:  # Se True, também pega 'error_enrichment' para tentar de novo
                    query_statuses.append('error_enrichment')

                placeholders = ','.join('?' * len(query_statuses))
                sql_query_base = f"SELECT document_id FROM enriched_documents WHERE status_enrichment IN ({placeholders})"

                query_params = list(query_statuses)  # Copia para modificar

                if specific_ids:  # Se IDs específicos são fornecidos, eles têm prioridade
                    ids_placeholders = ','.join('?' * len(specific_ids))
                    sql_query = f"{sql_query_base} AND document_id IN ({ids_placeholders}) ORDER BY created_at ASC"
                    query_params.extend(list(str(sid).strip() for sid in specific_ids if sid and str(sid).strip()))
                    logger.info(
                        f"Buscando IDs específicos {specific_ids} que estão em status {query_statuses} no SQLite.")
                else:
                    sql_query = f"{sql_query_base} ORDER BY created_at ASC"
                    if num_docs_to_process is not None and num_docs_to_process > 0:
                        sql_query += f" LIMIT ?"
                        query_params.append(num_docs_to_process)
                    logger.info(
                        f"Buscando até {num_docs_to_process if num_docs_to_process else 'todos'} documentos com status {query_statuses} no SQLite.")

                cursor.execute(sql_query, query_params)
                docs_to_enrich_ids_final_list = [row["document_id"] for row in cursor.fetchall()]
        except sqlite3.Error as e_fetch_all:
            logger.error(f"Erro ao buscar documentos para enriquecimento do SQLite: {e_fetch_all}", exc_info=True)
        finally:
            if conn_fetch: conn_fetch.close()

        if not docs_to_enrich_ids_final_list:
            logger.info(
                "Nenhum documento encontrado no SQLite para enriquecimento (com base nos critérios: status, specific_ids).")
        else:
            logger.info(
                f"Encontrados {len(docs_to_enrich_ids_final_list)} documentos no SQLite para enriquecimento: {docs_to_enrich_ids_final_list}")
            batch_size = settings.BATCH_SIZE_LLM_ENRICHMENT
            for i in range(0, len(docs_to_enrich_ids_final_list), batch_size):
                batch_ids = docs_to_enrich_ids_final_list[i:i + batch_size]
                logger.info(
                    f"Processando lote de enriquecimento {i // batch_size + 1}/{(len(docs_to_enrich_ids_final_list) + batch_size - 1) // batch_size} com {len(batch_ids)} IDs: {batch_ids}")
                self.enrich_batch(batch_ids)
                logger.info(f"Lote de enriquecimento concluído. Pausando...")
                time.sleep(1)

        logger.info("Pipeline de enriquecimento de metadados (SQLite) concluído.")

        # Chama a nova função para indexar no Elasticsearch
        self.index_enriched_documents_to_elasticsearch()


if __name__ == '__main__':
    logger.info("--- Executando MetadataEnricher Standalone (com Google Sheets e Indexação ES) ---")

    gs_instance = None
    llm_instance = None
    es_instance = None  # Nova instância
    enricher = None

    # Variável global para _insert_initial_documents_from_parser (solução simples para __main__)
    force_reprocess_errors_flag = True  # Mude para False se não quiser reprocessar erros por padrão ao rodar standalone

    try:
        # 1. Inicializar GlossaryServiceGS
        gs_instance = GlossaryServiceGS(
            service_account_path=settings.GCP_SERVICE_ACCOUNT_KEY_PATH,
            gsheet_id=settings.GLOBAL_GLOSSARY_GSHEET_ID,
            gsheet_tab_name=settings.GLOBAL_GLOSSARY_GSHEET_TAB_NAME
        )
        if not gs_instance.glossary_data:
            logger.warning(f"GlossaryServiceGS não carregou dados da planilha '{settings.GLOBAL_GLOSSARY_GSHEET_ID}'.")

        # 2. Inicializar LLMService
        llm_instance = LLMService(glossary_service=gs_instance)

        # 3. Inicializar ElasticsearchService
        es_instance = ElasticsearchService(
            hosts=settings.ELASTICSEARCH_HOSTS,
            user=settings.ELASTICSEARCH_USER,
            password=settings.ELASTICSEARCH_PASSWORD,
            request_timeout=30  # Exemplo, pode vir de settings
        )
        # Opcional: Verificar/criar o índice principal aqui também se o run_ingestion_pipeline não for executado antes
        # logger.info(f"Verificando/Criando índice ES: {settings.ELASTICSEARCH_INDEX_NAME} se não existir (standalone)...")
        # es_instance.create_index_if_not_exists(index_name=settings.ELASTICSEARCH_INDEX_NAME)

        logger.info(
            "Serviços GlossaryServiceGS, LLMService e ElasticsearchService instanciados para execução standalone.")

    except FileNotFoundError as fnf_err:
        if hasattr(settings, 'GCP_SERVICE_ACCOUNT_KEY_PATH') and str(fnf_err).count(
                settings.GCP_SERVICE_ACCOUNT_KEY_PATH):
            logger.critical(
                f"Arquivo de credenciais GCP não encontrado em {settings.GCP_SERVICE_ACCOUNT_KEY_PATH}. Detalhes: {fnf_err}",
                exc_info=True)
        else:
            logger.critical(f"Arquivo não encontrado durante inicialização de serviços: {fnf_err}", exc_info=True)
        sys.exit(1)
    except gspread.exceptions.GSpreadException as gspread_err:
        logger.critical(f"Erro ao acessar Google Sheets durante inicialização de serviços: {gspread_err}",
                        exc_info=True)
        sys.exit(1)
    except Exception as e_service_init:  # Captura outros erros, como falha de conexão ES
        logger.critical(f"Falha ao instanciar serviços para MetadataEnricher: {e_service_init}", exc_info=True)
        sys.exit(1)

    if gs_instance and llm_instance and es_instance:
        enricher = MetadataEnricher(
            llm_service_instance=llm_instance,
            glossary_service_instance=gs_instance,
            es_service_instance=es_instance  # Passa a instância do ES
        )

        logger.info(
            f"--- INICIANDO PIPELINE COMPLETO (SQLite + Indexação ES) COM METADADOS DE: GSheet ID {settings.SOURCE_METADATA_GSHEET_ID} ---")

        # Exemplo para testar um ID específico (ex: seu novo doc '713')
        # Certifique-se que o ID '713' está na sua planilha de metadados.
        # ids_para_teste = ["713"]
        # logger.info(f"Processando apenas IDs específicos: {ids_para_teste}")
        # enricher.run_enrichment_pipeline(force_reprocess_errors=True, specific_ids=ids_para_teste)

        # Para processar todos os pendentes/com erro (com base no que foi lido da planilha de metadados)
        # O `force_reprocess_errors=True` fará com que documentos marcados como 'error_enrichment' no SQLite
        # sejam re-tentados no passo de enrich_batch.
        # Também influencia _insert_initial_documents_from_parser se o documento já existe no SQLite.
        enricher.run_enrichment_pipeline(force_reprocess_errors=force_reprocess_errors_flag)

    else:
        logger.error(
            "Não foi possível criar todas as instâncias de serviço (LLM, Glossary, ES). Encerrando execução standalone.")

    logger.info("--- Execução Standalone do MetadataEnricher (com Indexação ES) Concluída ---")