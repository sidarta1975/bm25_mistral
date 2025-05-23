# python_backend_services/data_ingestion/tag_extractor.py
from typing import List, Dict, Set, Optional  # Added Optional here
import re
import logging

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')
logger = logging.getLogger(__name__)


# --- Conceptual Tagging Strategies ---
# You will need to define and implement your actual tagging strategy.
# Below are some placeholder ideas.

def extract_tags_from_filename(file_name: str) -> List[str]:
    """
    Conceptual: Extracts tags based on keywords in the filename.
    Example: "peticao_alimentos_urgente.txt" -> ["alimentos", "urgente"]
    """
    tags: Set[str] = set()
    # Simple example: split by underscore, remove "peticao", convert to lowercase
    parts = file_name.lower().replace(".txt", "").split('_')
    for part in parts:
        if part not in ["peticao", "acao", "de", "do", "da"] and len(part) > 2:  # Basic stopword list
            tags.add(part)
    return sorted(list(tags))


def extract_tags_from_content_keywords(
        document_content: str,
        tag_keyword_map: Dict[str, List[str]]  # Dict and List are from typing
) -> List[str]:
    """
    Conceptual: Extracts tags by searching for predefined keywords in the document content.
    The tag_keyword_map maps a found keyword (lowercase) to one or more tags.
    Example: if "divórcio consensual" is in content, and map has
             "divórcio consensual": ["divórcio", "direito de família", "consensual"]
             it would add these tags.

    Args:
        document_content (str): The text content of the document.
        tag_keyword_map (Dict[str, List[str]]): A dictionary where keys are keywords
                                                 (expected to be lowercase) and values are lists of tags.
                                                 Example: {"usucapião": ["propriedade", "civil"]}

    Returns:
        List[str]: A list of unique tags found based on keywords.
    """
    if not document_content or not tag_keyword_map:
        return []

    content_lower = document_content.lower()
    found_tags: Set[str] = set()

    for keyword, tags_for_keyword in tag_keyword_map.items():
        # Use regex for whole word matching to avoid partial matches (e.g., "art" in "party")
        # Pattern: \bkeyword\b
        try:
            if re.search(r'\b' + re.escape(keyword) + r'\b', content_lower):
                for tag in tags_for_keyword:
                    found_tags.add(tag.lower().strip())  # Normalize tags
        except re.error as e:
            logger.warning(f"Regex error for keyword '{keyword}': {e}. Skipping this keyword for tagging.")

    return sorted(list(found_tags))


def extract_tags_from_embedded_markers(document_content: str) -> List[str]:
    """
    Conceptual: Extracts tags if they are embedded in the document content
    using a specific marker, e.g., "TAGS: [tag1, tag2, tag3]".
    """
    tags: Set[str] = set()
    # Example regex: looks for "TAGS:" followed by bracketed, comma-separated values
    match = re.search(r'TAGS:\s*\[([^\]]+)\]', document_content, re.IGNORECASE)
    if match:
        tags_str = match.group(1)
        raw_tags = [tag.strip().lower() for tag in tags_str.split(',')]
        tags.update(filter(None, raw_tags))  # Filter out empty strings
    return sorted(list(tags))


# --- Main Tagging Function ---
def apply_tagging_strategies(
        document_data: Dict[str, any],  # Dict is from typing, 'any' needs 'Any' from typing
        keyword_map_for_content_tagging: Optional[Dict[str, List[str]]] = None  # Optional, Dict, List are from typing
) -> List[str]:  # List is from typing
    """
    Applies various tagging strategies to a document.
    This function should be customized based on your chosen tagging methods.

    Args:
        document_data (Dict[str, any]): The dictionary representing a parsed document.
                                         Must contain "file_name" and "content".
        keyword_map_for_content_tagging (Optional[Dict[str, List[str]]]):
            A map of keywords to tags for content-based tagging.
            If None, this strategy is skipped.

    Returns:
        List[str]: A list of unique, sorted tags for the document.
    """
    all_tags: Set[str] = set()  # Set is from typing

    file_name = document_data.get("file_name", "")
    content = document_data.get("content", "")

    # Strategy 1: Tags from filename (example)
    if file_name:
        tags_from_name = extract_tags_from_filename(file_name)
        all_tags.update(tags_from_name)
        if tags_from_name:
            logger.debug(f"Tags from filename '{file_name}': {tags_from_name}")

    # Strategy 2: Tags from embedded markers in content (example)
    if content:
        tags_from_markers = extract_tags_from_embedded_markers(content)
        all_tags.update(tags_from_markers)
        if tags_from_markers:
            logger.debug(f"Tags from embedded markers in doc ID {document_data.get('id', 'N/A')}: {tags_from_markers}")

    # Strategy 3: Tags from keywords in content (example)
    if content and keyword_map_for_content_tagging:
        tags_from_keywords = extract_tags_from_content_keywords(content, keyword_map_for_content_tagging)
        all_tags.update(tags_from_keywords)
        if tags_from_keywords:
            logger.debug(f"Tags from content keywords in doc ID {document_data.get('id', 'N/A')}: {tags_from_keywords}")

    final_tags = sorted(list(all_tags))
    logger.info(f"Applied tagging for doc ID {document_data.get('id', 'N/A')}. Final tags: {final_tags}")
    return final_tags


def process_documents_for_tags(
        documents: List[Dict[str, any]],  # List, Dict are from typing
        keyword_map_for_content_tagging: Optional[Dict[str, List[str]]] = None  # Optional, Dict, List are from typing
) -> List[Dict[str, any]]:  # List, Dict are from typing
    """
    Iterates through documents and applies tagging strategies to each.
    """
    logger.info(f"Processing {len(documents)} documents for tagging...")
    for doc_data in documents:
        existing_tags = set(doc_data.get("tags", []))  # Using built-in set
        newly_extracted_tags = apply_tagging_strategies(doc_data, keyword_map_for_content_tagging)
        existing_tags.update(newly_extracted_tags)
        doc_data["tags"] = sorted(list(existing_tags))  # Using built-in list

    logger.info("Finished processing documents for tags.")
    return documents


if __name__ == '__main__':
    # Example usage:
    import sys
    import os

    try:
        from app.core.config import settings  # Assuming relative import for app works from context

        KEYWORD_TAG_MAP = settings.TAG_KEYWORDS_MAP
        print("Using keyword tag map from settings.")
    except ImportError:
        print("Could not import settings. Using a default keyword map for testing.")
        KEYWORD_TAG_MAP = {
            "alimentos": ["direito de família", "pensão alimentícia"],
            "divórcio": ["direito de família", "dissolução"],
            "usucapião": ["civil", "propriedade", "direitos reais"],
            "contrato de aluguel": ["civil", "contratos", "locação"]
        }

    example_docs_for_tagging = [
        {
            "id": "docA",
            "file_name": "peticao_alimentos_urgente_modelo.txt",
            "content": "Esta é uma petição de alimentos. Discute o direito de família e a necessidade de pensão alimentícia. TAGS: [familia, alimentos, urgente]"
        },
        {
            "id": "docB",
            "file_name": "contrato_locacao_residencial.txt",
            "content": "Segue modelo de contrato de aluguel para fins residenciais. Envolve direito civil."
        },
        {
            "id": "docC",
            "file_name": "defesa_usucapiao.txt",
            "content": "Defesa em ação de usucapião de imóvel urbano."
        }
    ]

    processed_docs_with_tags = process_documents_for_tags(example_docs_for_tagging, KEYWORD_TAG_MAP)
    print("\n--- Processed Documents with Tags ---")
    for p_doc in processed_docs_with_tags:
        print(f"Document ID: {p_doc['id']}, Tags: {p_doc['tags']}")
