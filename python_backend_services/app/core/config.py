# python_backend_services/app/core/config.py
import os
from dotenv import load_dotenv
from typing import List, Optional, Dict

load_dotenv()


class Settings:
    # --- Project Root ---
    PROJECT_ROOT_DIR: str = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    SHARED_DATA_DIR: str = os.path.join(PROJECT_ROOT_DIR, 'shared_data')
    SOURCE_DOCS_DIR: str = os.path.join(PROJECT_ROOT_DIR, 'source_documents')

    # --- Google Cloud Service Account ---
    # Certifique-se de que o arquivo JSON esteja na pasta 'shared_data' ou ajuste o caminho.
    GCP_SERVICE_ACCOUNT_KEY_FILENAME: str = "txtparagdoc-b55767d3b447.json"  # Nome do seu arquivo JSON
    GCP_SERVICE_ACCOUNT_KEY_PATH: str = os.path.join(SHARED_DATA_DIR, GCP_SERVICE_ACCOUNT_KEY_FILENAME)

    # --- Google Sheets Configuration ---
    # ID da planilha que substitui source_metadata.tsv
    SOURCE_METADATA_GSHEET_ID: str = "1gJgYXiQF49PfFkjY3o76kQ_qaPkDZXmWJb3VU_WCnPs"
    # Nome da aba na planilha de metadados (ajuste se for diferente)
    SOURCE_METADATA_GSHEET_TAB_NAME: str = "Página1"

    # ID da planilha que substitui global_glossary.tsv
    GLOBAL_GLOSSARY_GSHEET_ID: str = "1q3bQkkfdcIkSMQLSc0SG5-A5Xgj8sExfBhHga-SHx9E"
    # Nome da aba na planilha do glossário (ajuste se for diferente)
    GLOBAL_GLOSSARY_GSHEET_TAB_NAME: str = "Sheet1"

    # --- Data Paths (Manter SOURCE_DOCS_BASE_DIR se os arquivos .txt ainda são locais) ---
    SOURCE_DOCS_BASE_DIR: str = os.getenv(
        "SOURCE_DOCS_BASE_DIR",
        os.path.join(SOURCE_DOCS_DIR, 'petitions')
    )

    # SQLITE_DB_PATH ainda é relevante para o metadata_enricher
    SQLITE_DB_PATH: str = os.getenv(
        "SQLITE_DB_PATH",
        os.path.join(SHARED_DATA_DIR, 'enriched_documents.sqlite')
    )

    # --- Elasticsearch Configuration (Manter como estava ou conforme sua necessidade de teste) ---
    ELASTICSEARCH_HOSTS: List[str] = [os.getenv("ELASTICSEARCH_HOST", "http://localhost:9200")]
    # Usando o índice de teste que funcionou anteriormente ou o seu principal
    ELASTICSEARCH_INDEX_NAME: str = os.getenv("ELASTICSEARCH_INDEX_NAME_TEST", "pytest_reintegracao_index")
    ELASTICSEARCH_USER: Optional[str] = os.getenv("ELASTICSEARCH_USER")
    ELASTICSEARCH_PASSWORD: Optional[str] = os.getenv("ELASTICSEARCH_PASSWORD")
    ELASTICSEARCH_REQUEST_TIMEOUT: int = int(os.getenv("ELASTICSEARCH_REQUEST_TIMEOUT", "30"))

    # --- Ollama LLM Configuration ---
    OLLAMA_API_URL: str = os.getenv("OLLAMA_API_URL", "http://localhost:11434/api/generate")
    OLLAMA_EMBEDDINGS_API_URL: str = os.getenv("OLLAMA_EMBEDDINGS_API_URL", "http://localhost:11434/api/embeddings")
    OLLAMA_MODEL_NAME: str = os.getenv("OLLAMA_MODEL_NAME", "mistral:7b")
    OLLAMA_REQUEST_TIMEOUT: int = int(os.getenv("OLLAMA_REQUEST_TIMEOUT", "180"))

    # --- Embedding Settings ---
    EMBEDDING_DIMENSIONS: int = int(os.getenv("EMBEDDING_DIMENSIONS", "4096"))

    # --- Logging ---
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "DEBUG").upper() # Manter DEBUG para facilitar o diagnóstico

    # --- Search Settings ---
    CANDIDATES_FOR_LLM_RERANK: int = int(os.getenv("CANDIDATES_FOR_LLM_RERANK", "3"))

    # --- Data Ingestion Settings ---
    BATCH_SIZE_LLM_ENRICHMENT: int = int(os.getenv("BATCH_SIZE_LLM_ENRICHMENT_TEST", "1"))

    TAG_KEYWORDS_MAP: Dict[str, List[str]] = {
        "alimentos": ["direito de família", "pensão alimentícia", "fixação de alimentos"],
        "pensão alimentícia": ["direito de família", "pensão alimentícia", "execução de alimentos"],
        "divórcio": ["direito de família", "dissolução de casamento", "partilha de bens"],
        "dano moral": ["responsabilidade civil", "indenização", "danos"],
    }

settings = Settings()