# python_backend_services/data_ingestion/document_parser.py
import os
import glob
from typing import List, Dict, Optional
import logging

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')
logger = logging.getLogger(__name__)


def extract_document_id_from_path(file_path: str) -> str:
    """
    Extracts a unique document ID from the file path.
    By default, uses the filename without extension.
    You might want to customize this for more complex ID generation.
    """
    return os.path.splitext(os.path.basename(file_path))[0]


def read_txt_file(file_path: str) -> Optional[str]:
    """
    Reads the content of a .txt file.

    Args:
        file_path (str): The path to the .txt file.

    Returns:
        Optional[str]: The content of the file as a string, or None if an error occurs.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        return content
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        return None
    except Exception as e:
        logger.error(f"Error reading file {file_path}: {e}")
        return None


def parse_document(file_path: str) -> Optional[Dict[str, any]]:
    """
    Parses a single document file.
    Extracts content and basic metadata.

    Args:
        file_path (str): The path to the document file.

    Returns:
        Optional[Dict[str, any]]: A dictionary containing document data (id, content, path),
                                   or None if parsing fails.
    """
    logger.info(f"Parsing document: {file_path}")
    content = read_txt_file(file_path)
    if content is None:
        return None

    doc_id = extract_document_id_from_path(file_path)

    # Basic metadata, can be expanded
    document_data = {
        "id": doc_id,  # Unique identifier for the document
        "file_path": file_path,
        "file_name": os.path.basename(file_path),
        "content": content,
        "tags": [],  # To be populated by tag_extractor.py
        "glossary_terms_found": [],  # To be populated by glossary_processor.py
        # Add other metadata fields as needed, e.g., creation_date, source_folder
    }

    # Example: Extracting area of law from parent folder if structure is source_documents/petitions/area_law_1/file.txt
    try:
        # Assuming 'petitions' is a direct child of source_documents_dir
        # and area_law folders are direct children of 'petitions'
        relative_path = os.path.relpath(file_path,
                                        os.path.join(os.path.dirname(os.path.dirname(file_path)), "petitions"))
        path_parts = relative_path.split(os.sep)
        if len(path_parts) > 1:  # Check if there's a subfolder for area of law
            document_data["area_of_law"] = path_parts[0]
    except Exception as e:
        logger.warning(f"Could not extract area_of_law from path {file_path}: {e}")

    return document_data


def discover_and_parse_documents(source_dir: str) -> List[Dict[str, any]]:
    """
    Discovers all .txt files in the source directory (and its subdirectories)
    and parses them.

    Args:
        source_dir (str): The root directory containing .txt petition files.

    Returns:
        List[Dict[str, any]]: A list of dictionaries, where each dictionary
                              represents a parsed document.
    """
    if not os.path.isdir(source_dir):
        logger.error(f"Source directory not found or is not a directory: {source_dir}")
        return []

    logger.info(f"Discovering documents in: {source_dir}")
    # Using glob to find all .txt files recursively
    # The pattern '/**/' ensures recursive search in Python 3.5+
    file_paths = glob.glob(os.path.join(source_dir, '**', '*.txt'), recursive=True)

    parsed_documents = []
    if not file_paths:
        logger.warning(f"No .txt files found in {source_dir} or its subdirectories.")
        return []

    logger.info(f"Found {len(file_paths)} .txt files to parse.")

    for file_path in file_paths:
        doc_data = parse_document(file_path)
        if doc_data:
            parsed_documents.append(doc_data)

    logger.info(f"Successfully parsed {len(parsed_documents)} documents.")
    return parsed_documents


if __name__ == '__main__':
    # Example usage:
    # This assumes your config.py is accessible and SOURCE_DOCUMENTS_DIR is set.
    # You might need to adjust paths if running this file directly for testing.
    import sys

    # Add project root to sys.path if running directly for testing imports
    # sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

    try:
        from app.core.config import settings  # Assuming config.py is in app/core/

        SOURCE_DIR = settings.SOURCE_DOCUMENTS_DIR
        print(f"Using source directory from settings: {SOURCE_DIR}")
    except ImportError:
        print("Could not import settings. Using a default path for testing.")
        # Fallback for direct execution if settings are not easily importable
        # Adjust this path to point to your actual test documents for direct run
        SOURCE_DIR = "../../source_documents/petitions/"
        if not os.path.exists(SOURCE_DIR):
            # Create dummy files for testing if they don't exist
            os.makedirs(os.path.join(SOURCE_DIR, "civil"), exist_ok=True)
            with open(os.path.join(SOURCE_DIR, "civil", "doc1_civil.txt"), "w", encoding="utf-8") as f:
                f.write("Este é o conteúdo da petição civil 1 sobre usucapião.")
            with open(os.path.join(SOURCE_DIR, "doc2_geral.txt"), "w", encoding="utf-8") as f:
                f.write("Conteúdo geral do documento 2.")

    documents = discover_and_parse_documents(SOURCE_DIR)
    if documents:
        print(f"\n--- Example Parsed Documents ({len(documents)} found) ---")
        for i, doc in enumerate(documents[:2]):  # Print first 2
            print(f"\nDocument {i + 1}:")
            print(f"  ID: {doc.get('id')}")
            print(f"  File Name: {doc.get('file_name')}")
            print(f"  Area of Law: {doc.get('area_of_law', 'N/A')}")
            print(f"  Content (first 50 chars): {doc.get('content', '')[:50]}...")
    else:
        print("No documents were parsed.")
