# python_backend_services/app/core/config.py
import os
from dotenv import load_dotenv
from typing import List, Optional  # <--- ADICIONE Optional AQUI

load_dotenv()


class Settings:
    # --- Data Paths ---
    # Base directory for source .txt documents (petitions)
    SOURCE_DOCS_BASE_DIR: str = os.getenv(
        "SOURCE_DOCS_BASE_DIR",
        "/home/sidarta/PycharmProjects/bm25_mistral/python_backend_services/source_documents/petitions/"
    )
    # Path to the TSV file containing document metadata
    METADATA_TSV_PATH: str = os.getenv(
        "METADATA_TSV_PATH",
        os.path.join(os.path.dirname(__file__), '..', '..', 'shared_data', 'relatorio_classificacao.tsv')
    )
    # Path to the glossary file (still used by glossary_service.py)
    GLOSSARY_FILE_PATH: str = os.getenv(
        "GLOSSARY_FILE_PATH",
        os.path.join(os.path.dirname(__file__), '..', '..', 'shared_data', 'glossario.tsv')
    )

    # --- Elasticsearch Configuration ---
    ELASTICSEARCH_HOSTS: List[str] = [os.getenv("ELASTICSEARCH_HOST", "http://localhost:9200")]
    ELASTICSEARCH_INDEX_NAME: str = os.getenv("ELASTICSEARCH_INDEX_NAME", "legal_petitions_mvp_index")
    ELASTICSEARCH_USER: Optional[str] = os.getenv("ELASTICSEARCH_USER")  # Agora Optional será reconhecido
    ELASTICSEARCH_PASSWORD: Optional[str] = os.getenv("ELASTICSEARCH_PASSWORD")  # E aqui também

    # --- LLM / Embedding Configuration ---
    OLLAMA_API_URL: str = os.getenv("OLLAMA_API_URL", "http://localhost:11434/api/generate")
    OLLAMA_MODEL_NAME: str = os.getenv("OLLAMA_MODEL_NAME", "mistral:7b")
    OLLAMA_REQUEST_TIMEOUT: int = int(os.getenv("OLLAMA_REQUEST_TIMEOUT", "120"))

    EMBEDDING_DIMENSIONS: int = int(os.getenv("EMBEDDING_DIMENSIONS", "4096"))

    # --- Logging ---
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO").upper()

    # --- Search Settings ---
    BM25_TOP_N_RESULTS: int = int(os.getenv("BM25_TOP_N_RESULTS", "10"))


settings = Settings()

# Para testar se os caminhos estão corretos a partir deste arquivo:
if __name__ == '__main__':
    print(f"SOURCE_DOCS_BASE_DIR: {settings.SOURCE_DOCS_BASE_DIR}")
    print(f"METADATA_TSV_PATH: {settings.METADATA_TSV_PATH}")
    print(f"GLOSSARY_FILE_PATH: {settings.GLOSSARY_FILE_PATH}")
    print(f"Exists METADATA_TSV_PATH? {os.path.exists(settings.METADATA_TSV_PATH)}")
    print(f"Is dir SOURCE_DOCS_BASE_DIR? {os.path.isdir(settings.SOURCE_DOCS_BASE_DIR)}")
    print(f"Elasticsearch User: {settings.ELASTICSEARCH_USER}")  # Testando o Optional