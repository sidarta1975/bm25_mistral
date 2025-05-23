# python_backend_services/data_ingestion/run_ingestion.py
import logging
import time
import os
import sys
import argparse # Make sure argparse is imported

# --- Setup sys.path if running script directly ---
# This allows importing modules from the project root (e.g., app.core.config)
# when 'run_ingestion.py' is executed as the main script.
# Adjust the number of '..' if your script is nested deeper.
# current_dir = os.path.dirname(os.path.abspath(__file__))
# project_root = os.path.abspath(os.path.join(current_dir, '..')) # Assuming data_ingestion is one level below project_root
# if project_root not in sys.path:
#    sys.path.insert(0, project_root)
# --- End sys.path setup ---

try:
    # These imports assume you are running from the 'bm25_mistral' directory
    # using 'python -m python_backend_services.data_ingestion.run_ingestion'
    from python_backend_services.app.core.config import settings
    from python_backend_services.data_ingestion.document_parser import discover_and_parse_documents
    from python_backend_services.data_ingestion.glossary_processor import load_glossary_terms, process_documents_with_glossary
    from python_backend_services.data_ingestion.tag_extractor import process_documents_for_tags
    from python_backend_services.data_ingestion.indexer_service import ElasticsearchService
except ImportError as e:
    print(f"Error importing modules. Current sys.path: {sys.path}")
    print(f"Details: {e}")
    print("Ensure you are running from the project root ('bm25_mistral') using 'python -m python_backend_services.data_ingestion.run_ingestion'")
    print("Also ensure all necessary __init__.py files exist in your package directories.")
    sys.exit(1)


# Configure logging for the ingestion process
logging.basicConfig(level=settings.LOG_LEVEL,
                    format='%(asctime)s - %(levelname)s - %(name)s - %(module)s - %(message)s')
logger = logging.getLogger(__name__)  # Get a logger specific to this module


def main_ingestion_pipeline(recreate_index: bool = False):
    """
    Main pipeline for discovering, parsing, processing, and indexing documents.
    Args:
        recreate_index (bool): If True, deletes the existing Elasticsearch index before creating a new one.
                               Use with caution in production.
    """
    start_time = time.time()
    logger.info("Starting data ingestion pipeline...")

    # --- 0. Initialize Elasticsearch Service ---
    try:
        es_service = ElasticsearchService(
            es_hosts=settings.ELASTICSEARCH_HOSTS,
            es_user=settings.ELASTICSEARCH_USER,
            es_password=settings.ELASTICSEARCH_PASSWORD
        )
    except ConnectionError:
        logger.critical("Failed to connect to Elasticsearch. Aborting ingestion pipeline.")
        return
    except Exception as e:
        logger.critical(f"Failed to initialize ElasticsearchService: {e}. Aborting.")
        return

    # --- 1. (Optional) Recreate Index ---
    if recreate_index:
        logger.warning(f"Recreate_index is True. Attempting to delete index '{settings.ELASTICSEARCH_INDEX_NAME}'...")
        if es_service.delete_index(settings.ELASTICSEARCH_INDEX_NAME):
            logger.info(f"Index '{settings.ELASTICSEARCH_INDEX_NAME}' deleted. It will be recreated.")
            time.sleep(1)  # Give Elasticsearch a moment
        else:
            logger.error(
                f"Failed to delete index '{settings.ELASTICSEARCH_INDEX_NAME}'. It might not exist or an error occurred.")

    # Ensure index exists with correct mappings
    try:
        es_service.create_index_if_not_exists(settings.ELASTICSEARCH_INDEX_NAME)
    except Exception as e:
        logger.critical(
            f"Failed to create or verify Elasticsearch index '{settings.ELASTICSEARCH_INDEX_NAME}': {e}. Aborting.")
        return

    # --- 2. Discover and Parse Documents ---
    logger.info(f"Loading documents from: {settings.SOURCE_DOCUMENTS_DIR}")
    documents = discover_and_parse_documents(settings.SOURCE_DOCUMENTS_DIR)
    if not documents:
        logger.warning("No documents found or parsed. Exiting pipeline.")
        return
    logger.info(f"Parsed {len(documents)} documents.")

    # --- 3. Load Glossary and Process Documents ---
    logger.info(f"Loading glossary from: {settings.GLOSSARY_FILE_PATH}")
    glossary_terms_set = load_glossary_terms(settings.GLOSSARY_FILE_PATH)
    if not glossary_terms_set:
        logger.warning("Glossary is empty or could not be loaded. Proceeding without glossary term tagging.")

    documents = process_documents_with_glossary(documents, glossary_terms_set)
    logger.info("Applied glossary terms to documents.")

    # --- 4. Apply Tagging Strategies ---
    # The keyword map for content tagging could come from settings or another source
    logger.info("Applying tagging strategies to documents...")
    # Assuming settings.TAG_KEYWORDS_MAP is defined in your config.py as shown in the example
    keyword_map_for_tagging = getattr(settings, 'TAG_KEYWORDS_MAP', {})
    documents = process_documents_for_tags(documents, keyword_map_for_tagging)
    logger.info("Applied tags to documents.")

    # --- 5. Bulk Index Documents into Elasticsearch ---
    logger.info(f"Indexing {len(documents)} processed documents into '{settings.ELASTICSEARCH_INDEX_NAME}'...")
    success_count, errors = es_service.bulk_index_documents(settings.ELASTICSEARCH_INDEX_NAME, documents)

    if errors:
        logger.error(f"Encountered {len(errors)} errors during bulk indexing.")
        # Log a few example errors
        for i, err_detail in enumerate(errors[:5]):
            logger.error(f"Indexing Error {i + 1}: {err_detail}")

    logger.info(f"Successfully indexed {success_count} out of {len(documents)} documents.")

    end_time = time.time()
    logger.info(f"Data ingestion pipeline completed in {end_time - start_time:.2f} seconds.")
    logger.info(f"Total documents processed: {len(documents)}")
    logger.info(f"Successfully indexed in Elasticsearch: {success_count}")


if __name__ == "__main__":
    # Example: To run with index recreation:
    # python python_backend_services/data_ingestion/run_ingestion.py --recreate-index

    import argparse

    parser = argparse.ArgumentParser(description="Run the data ingestion pipeline for legal petitions.")
    parser.add_argument(
        "--recreate-index",
        action="store_true",
        help="If set, deletes and recreates the Elasticsearch index before ingestion."
    )
    args = parser.parse_args()

    # Ensure that the current working directory is the project root `bm25_mistral`
    # or that `python_backend_services` is in PYTHONPATH for imports to work correctly.
    # If you run `python python_backend_services/data_ingestion/run_ingestion.py`
    # from `bm25_mistral/`, imports should work if `python_backend_services`
    # is structured as a package (contains __init__.py files).

    # A simple way to adjust path for direct execution from `python_backend_services/data_ingestion/`
    # This assumes `app.core.config` is at `../app/core/config.py` relative to this script
    # and `data_ingestion.document_parser` is in the same directory.
    # If your project root `bm25_mistral` is not in sys.path,
    # direct execution might fail to find `app.core.config`.
    # It's often better to run scripts as modules from the project root if imports are tricky:
    # python -m python_backend_services.data_ingestion.run_ingestion --recreate-index

    # For direct script execution from any location, more robust path handling might be needed:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # Assuming 'python_backend_services' is the parent of 'data_ingestion'
    python_backend_services_dir = os.path.dirname(script_dir)
    # Assuming 'bm25_mistral' is the parent of 'python_backend_services'
    project_root_dir = os.path.dirname(python_backend_services_dir)

    # Add python_backend_services to sys.path to allow imports like `from app.core...`
    # and `from data_ingestion...`
    if python_backend_services_dir not in sys.path:
        sys.path.insert(0, python_backend_services_dir)
    # If your 'app' and 'data_ingestion' are direct children of project_root,
    # and project_root is what you want for top-level package name:
    if project_root_dir not in sys.path:  # If you treat bm25_mistral as a package root
        pass  # sys.path.insert(0, project_root_dir) -> this might cause issues if not intended.
        # Usually, the directory containing your top-level packages (like 'app', 'data_ingestion')
        # should be in sys.path. Here, `python_backend_services_dir` acts as that.

    main_ingestion_pipeline(recreate_index=args.recreate_index)
