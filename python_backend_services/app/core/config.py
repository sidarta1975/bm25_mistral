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

    # --- Data Paths ---
    SOURCE_DOCS_BASE_DIR: str = os.getenv(
        "SOURCE_DOCS_BASE_DIR",
        os.path.join(SOURCE_DOCS_DIR, 'petitions') # Mantém apontando para a pasta petitions
    )
    # REVERTIDO PARA USAR O ARQUIVO DE METADADOS PRINCIPAL
    METADATA_TSV_PATH: str = os.getenv(
        "METADATA_TSV_PATH",
        os.path.join(SHARED_DATA_DIR, 'source_metadata.tsv')
    )
    GLOSSARY_FILE_PATH: str = os.getenv(
        "GLOSSARY_FILE_PATH",
        os.path.join(SHARED_DATA_DIR, 'global_glossary.tsv')
    )
    SQLITE_DB_PATH: str = os.getenv(
        "SQLITE_DB_PATH",
        os.path.join(SHARED_DATA_DIR, 'enriched_documents.sqlite')
    )

    # --- Elasticsearch Configuration ---
    ELASTICSEARCH_HOSTS: List[str] = [os.getenv("ELASTICSEARCH_HOST", "http://localhost:9200")]
    # REVERTIDO PARA USAR O NOME DO ÍNDICE PRINCIPAL
    ELASTICSEARCH_INDEX_NAME: str = os.getenv("ELASTICSEARCH_INDEX_NAME", "legal_petitions_v2") # ou v1, ou o seu nome de índice principal
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
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO").upper() # Pode voltar para INFO ou manter DEBUG se preferir

    # --- Search Settings ---
    CANDIDATES_FOR_LLM_RERANK: int = int(os.getenv("CANDIDATES_FOR_LLM_RERANK", "5"))

    # --- Data Ingestion Settings ---
    BATCH_SIZE_LLM_ENRICHMENT: int = int(os.getenv("BATCH_SIZE_LLM_ENRICHMENT", "5")) # Pode voltar para um valor maior

    TAG_KEYWORDS_MAP: Dict[str, List[str]] = {
        "alimentos": ["direito de família", "pensão alimentícia", "fixação de alimentos"],
        "pensão alimentícia": ["direito de família", "pensão alimentícia", "execução de alimentos"],
        "divórcio": ["direito de família", "dissolução de casamento", "partilha de bens"],
        "dano moral": ["responsabilidade civil", "indenização", "danos"],
    }

settings = Settings()