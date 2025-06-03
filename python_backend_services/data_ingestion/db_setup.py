# python_backend_services/data_ingestion/db_setup.py
import sqlite3
import logging
import os

try:
    from python_backend_services.app.core.config import settings # Usado para SQLITE_DB_PATH [cite: 6]
except ImportError:
    print(
        "db_setup.py: WARNING - Could not import 'settings' from project structure. Using fallback for SQLITE_DB_PATH.")
    class MockSettingsDBSetup:
        SQLITE_DB_PATH = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "shared_data", "enriched_documents_antigo.sqlite"))
    settings = MockSettingsDBSetup()

if not logging.getLogger().hasHandlers():
    log_level_to_use = "INFO"
    if hasattr(settings, 'LOG_LEVEL'):
        log_level_to_use = settings.LOG_LEVEL.upper()
    logging.basicConfig(level=log_level_to_use,
                        format='%(asctime)s - %(levelname)s - [%(name)s:%(lineno)d] - %(message)s')

logger = logging.getLogger(__name__)


def create_tables(): # [cite: 6]
    conn = None
    try:
        db_path = settings.SQLITE_DB_PATH # [cite: 6]
        logger.info(f"Conectando ao banco de dados SQLite em: {db_path}")

        db_dir = os.path.dirname(db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)
            logger.info(f"Diretório do banco de dados criado: {db_dir}")

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        logger.info("Conexão estabelecida. Criando tabelas se não existirem...")

        cursor.execute("""
                       CREATE TABLE IF NOT EXISTS enriched_documents
                       (
                           document_id TEXT PRIMARY KEY,
                           file_name TEXT,
                           content_path TEXT,
                           document_title_original TEXT,
                           summary_original TEXT,
                           first_lines_original TEXT,
                           document_category_original TEXT,
                           document_type_original TEXT,
                           legal_action_original TEXT,
                           legal_domain_original TEXT,      -- Corresponde ao conceito de "legal_domain" (F) do glossário, mas para o doc original
                           sub_areas_of_law_original TEXT,  -- Corresponde ao conceito de "sub_areas_of_law" (E) do glossário, mas para o doc original
                           jurisprudence_court_original TEXT,
                           version_original TEXT,
                           full_text_content TEXT,
                           document_title_llm TEXT,
                           summary1_llm TEXT,      -- Resumo técnico do DOCUMENTO. Análogo a "summary_tec" (C) do glossário (que é para TERMOS) [cite: 6]
                           summary2_llm TEXT,      -- Resumo leigo do DOCUMENTO. Análogo a "summary_public" (D) do glossário (que é para TERMOS) [cite: 6]
                           legal_domain_llm TEXT,    -- "legal_domain" (F) do DOCUMENTO, refinado por LLM [cite: 6]
                           sub_areas_of_law_llm TEXT,      -- "sub_areas_of_law" (E) do DOCUMENTO, refinado por LLM [cite: 6]
                           document_specific_terms TEXT,      -- Lista de "termo_juridico" (B) do glossário encontrados no documento [cite: 6]
                           status_enrichment TEXT DEFAULT 'pending',
                           llm_error_message TEXT,
                           created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                           updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                       )
                       """)
        logger.info("Tabela 'enriched_documents' verificada/criada.")

        cursor.execute("""
                       CREATE TABLE IF NOT EXISTS interaction_logs
                       (
                           log_id INTEGER PRIMARY KEY AUTOINCREMENT,
                           timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                           user_phone TEXT NOT NULL,
                           interaction_id TEXT UNIQUE,
                           user_message TEXT,
                           agent_response TEXT,
                           document_id_sent TEXT, -- Referencia document_id de enriched_documents
                           feedback_raw TEXT,
                           feedback_sentiment TEXT,
                           error_message TEXT,
                           FOREIGN KEY (document_id_sent) REFERENCES enriched_documents (document_id) ON DELETE SET NULL
                           )
                       """)
        logger.info("Tabela 'interaction_logs' verificada/criada.")

        cursor.execute("""
                       CREATE TABLE IF NOT EXISTS approved_petition_stats
                       (
                           stat_id INTEGER PRIMARY KEY AUTOINCREMENT,
                           document_id TEXT NOT NULL, -- Referencia document_id de enriched_documents
                           normalized_query_hash TEXT NOT NULL,
                           approval_count INTEGER DEFAULT 0,
                           last_approved_at DATETIME,
                           UNIQUE (document_id, normalized_query_hash),
                           FOREIGN KEY (document_id) REFERENCES enriched_documents (document_id) ON DELETE CASCADE
                           )
                       """)
        logger.info("Tabela 'approved_petition_stats' verificada/criada.")

        cursor.execute("""
                       CREATE TRIGGER IF NOT EXISTS update_enriched_documents_updated_at
                       AFTER UPDATE ON enriched_documents
                           FOR EACH ROW
                       BEGIN
                       UPDATE enriched_documents
                       SET updated_at = CURRENT_TIMESTAMP
                       WHERE document_id = OLD.document_id;
                       END;
                       """)
        logger.info("Trigger 'update_enriched_documents_updated_at' verificado/criado.")

        cursor.execute("CREATE INDEX IF NOT EXISTS idx_enriched_status ON enriched_documents (status_enrichment)")
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_interaction_user_time ON interaction_logs (user_phone, timestamp)")
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_approved_query_hash ON approved_petition_stats (normalized_query_hash)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_approved_doc_id ON approved_petition_stats (document_id)")

        conn.commit()
        logger.info("Criação/verificação de tabelas, triggers e índices concluída com sucesso.")
    except sqlite3.Error as e:
        logger.error(f"Erro ao criar/verificar tabelas SQLite: {e}", exc_info=True)
        if conn:
            conn.rollback()
    except AttributeError as ae:
        logger.error(f"Erro de atributo (provavelmente 'settings' não configurado corretamente): {ae}", exc_info=True)
    except Exception as ex:
        logger.error(f"Erro geral em create_tables: {ex}", exc_info=True)
    finally:
        if conn:
            conn.close()
            logger.info("Conexão SQLite fechada.")


if __name__ == '__main__':
    logger.info("Executando db_setup.py como script principal para criar/verificar tabelas.")
    create_tables() # [cite: 6]