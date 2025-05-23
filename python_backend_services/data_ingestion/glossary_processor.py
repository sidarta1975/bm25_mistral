# python_backend_services/data_ingestion/glossary_processor.py
import csv
from typing import List, Set, Dict
import logging
import re

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')
logger = logging.getLogger(__name__)


def load_glossary_terms(glossary_file_path: str) -> Set[str]:
    """
    Loads legal terms from a TSV glossary file.
    Assumes the term is in the second column (index 1) and skips the header.
    Terms are converted to lowercase.

    Args:
        glossary_file_path (str): Path to the TSV glossary file.

    Returns:
        Set[str]: A set of unique glossary terms in lowercase.
    """
    terms: Set[str] = set()
    try:
        with open(glossary_file_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f, delimiter='\t')
            try:
                next(reader)  # Skip the header row
            except StopIteration:
                logger.warning(f"Glossary file '{glossary_file_path}' is empty or has only a header.")
                return terms

            for row_number, row in enumerate(reader, 2):
                if row and len(row) > 1:
                    term = row[1].strip().lower()
                    if term and not term.startswith("=ai("):  # Basic filter
                        terms.add(term)
                # else:
                #     logger.debug(f"Skipping malformed or short row {row_number} in glossary: {row}")
        logger.info(f"Loaded {len(terms)} unique terms from glossary: {glossary_file_path}")
    except FileNotFoundError:
        logger.error(f"Glossary file not found: {glossary_file_path}")
    except Exception as e:
        logger.error(f"Error loading glossary from '{glossary_file_path}': {e}")
    return terms


def find_glossary_terms_in_document(document_content: str, glossary_terms: Set[str]) -> List[str]:
    """
    Finds which glossary terms are present in the document content.
    Uses simple substring matching after normalizing document content.
    Considers whole word matching to reduce false positives.

    Args:
        document_content (str): The text content of the document.
        glossary_terms (Set[str]): A set of glossary terms (lowercase).

    Returns:
        List[str]: A list of unique glossary terms found in the document.
    """
    if not document_content or not glossary_terms:
        return []

    # Normalize document content: lowercase and replace non-alphanumeric with spaces for word boundary checks
    # This helps in matching whole words.
    # We keep some punctuation that might be part of terms, e.g. "direito de família"
    # A more sophisticated approach might use NLP tokenization.
    normalized_content = " " + document_content.lower() + " "

    # Replace punctuation (except hyphens if terms might contain them) with spaces
    # This helps in creating word boundaries for regex.
    # This regex keeps alphanumeric, spaces, and hyphens. Other punctuation becomes a space.
    normalized_content_for_regex = re.sub(r'[^\w\s-]', ' ', document_content.lower())
    # Add spaces at the beginning and end to ensure boundary matching for terms at start/end of content
    normalized_content_for_regex = f" {normalized_content_for_regex} "

    found_terms: Set[str] = set()
    for term in glossary_terms:
        if not term:  # Skip empty terms from glossary if any
            continue
        # Use regex for whole word matching. \b matches word boundaries.
        # Escape the term in case it contains special regex characters.
        # The pattern ensures the term is surrounded by word boundaries.
        # Example: term "lei" should not match "eleitoral"
        # Pattern: \bterm\b
        try:
            # We search in normalized_content_for_regex which has clear word boundaries
            if re.search(r'\b' + re.escape(term) + r'\b', normalized_content_for_regex):
                found_terms.add(term)
        except re.error as e:
            logger.warning(f"Regex error for term '{term}': {e}. Skipping this term for matching.")

    return sorted(list(found_terms))


def process_documents_with_glossary(
        documents: List[Dict[str, any]],
        glossary_terms: Set[str]
) -> List[Dict[str, any]]:
    """
    Iterates through documents and adds a list of found glossary terms to each.

    Args:
        documents (List[Dict[str, any]]): List of parsed document dictionaries.
                                           Each dict must have a "content" key.
        glossary_terms (Set[str]): A set of glossary terms to search for.

    Returns:
        List[Dict[str, any]]: The list of documents, with each document dictionary
                              updated with a "glossary_terms_found" key.
    """
    logger.info(f"Processing {len(documents)} documents to find glossary terms...")
    for doc in documents:
        if "content" in doc and isinstance(doc["content"], str):
            found_in_doc = find_glossary_terms_in_document(doc["content"], glossary_terms)
            doc["glossary_terms_found"] = found_in_doc
            if found_in_doc:
                logger.debug(f"Found terms {found_in_doc} in document ID {doc.get('id', 'N/A')}")
        else:
            doc["glossary_terms_found"] = []
            logger.warning(f"Document ID {doc.get('id', 'N/A')} missing 'content' or content is not a string.")

    logger.info("Finished processing documents with glossary.")
    return documents


if __name__ == '__main__':
    # Example usage:
    import sys
    import os

    # sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

    try:
        from app.core.config import settings

        GLOSSARY_PATH = settings.GLOSSARY_FILE_PATH
        print(f"Using glossary path from settings: {GLOSSARY_PATH}")
    except ImportError:
        print("Could not import settings. Using a default path for testing glossary.")
        # Create a dummy glossary.tsv for direct execution testing
        GLOSSARY_PATH = "../../shared_data/glossario.tsv"
        os.makedirs(os.path.dirname(GLOSSARY_PATH), exist_ok=True)
        with open(GLOSSARY_PATH, "w", encoding="utf-8") as f:
            f.write("ID\tTermo Jurídico\tDefinição\n")
            f.write("1\tdireito de família\tRegras sobre relações familiares.\n")
            f.write("2\tusucapião\tAquisição de propriedade pelo tempo.\n")
            f.write("3\tpensão alimentícia\tValor para sustento.\n")
            f.write("4\t\t\t\n")  # Empty term test
            f.write("5\tguarda compartilhada\tResponsabilidade parental dividida.\n")

    # Load terms
    terms = load_glossary_terms(GLOSSARY_PATH)
    print(f"\nLoaded glossary terms: {terms}")

    # Example documents for testing
    example_docs_for_glossary = [
        {"id": "doc1",
         "content": "Este documento fala sobre o direito de família e a importância da pensão alimentícia."},
        {"id": "doc2", "content": "Trata-se de um caso de usucapião de um imóvel rural. A lei de usucapião é clara."},
        {"id": "doc3", "content": "Nenhuma menção a termos jurídicos conhecidos aqui."},
        {"id": "doc4", "content": "A guarda compartilhada foi decidida pelo juiz."},
        {"id": "doc5", "content": None},  # Test missing content
        {"id": "doc6", "content": "Direito De Família é complexo."}  # Test case insensitivity
    ]

    # Process documents
    processed_docs = process_documents_with_glossary(example_docs_for_glossary, terms)
    print("\n--- Processed Documents with Glossary Terms ---")
    for p_doc in processed_docs:
        print(f"Document ID: {p_doc['id']}, Found Terms: {p_doc['glossary_terms_found']}")

    # Test with a more complex sentence and term
    complex_content = "O processo de usucapião extraordinário exige posse mansa e pacífica. A lei é clara."
    found = find_glossary_terms_in_document(complex_content, {"usucapião", "lei"})
    print(f"\nTest complex content: '{complex_content}'")
    print(f"Found terms: {found}")  # Expected: ['lei', 'usucapião']
