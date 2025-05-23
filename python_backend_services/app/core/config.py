# python_backend_services/app/core/config.py
# This version is slightly simplified for Stage 1 focus,
# removing Ollama and complex tagging map for now.
import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    # --- Data Paths ---
    # Assuming your ingestion pipeline has already run and populated Elasticsearch.
    # These paths might not be directly used by the app in Stage 1,
    # but good to keep for consistency.
    SOURCE_DOCUMENTS_DIR: str = os.getenv("SOURCE_DOCUMENTS_DIR", "./python_backend_services/source_documents/petitions/")
    GLOSSARY_FILE_PATH: str = os.getenv("GLOSSARY_FILE_PATH", "./python_backend_services/shared_data/glossario.tsv")

    # --- Elasticsearch Configuration ---
    ELASTICSEARCH_HOSTS: list = [os.getenv("ELASTICSEARCH_HOST", "http://localhost:9200")]
    ELASTICSEARCH_INDEX_NAME: str = os.getenv("ELASTICSEARCH_INDEX_NAME", "legal_petitions_index") # Ensure this matches your ingested index
    ELASTICSEARCH_USER: str | None = os.getenv("ELASTICSEARCH_USER")
    ELASTICSEARCH_PASSWORD: str | None = os.getenv("ELASTICSEARCH_PASSWORD")

    # --- Logging ---
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO").upper()

    # --- Search Settings for Stage 1 ---
    BM25_TOP_N_RESULTS: int = int(os.getenv("BM25_TOP_N_RESULTS", "5")) # Number of results to return from ES

settings = Settings()

