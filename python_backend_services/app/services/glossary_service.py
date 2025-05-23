# python_backend_services/app/services/glossary_service.py
import csv
import logging
from typing import List, Dict, Optional
import re

try:
    from python_backend_services.app.core.config import settings
except ImportError:
    class FallbackSettingsGlossary:
        GLOSSARY_FILE_PATH = "./python_backend_services/shared_data/glossario.tsv"


    settings = FallbackSettingsGlossary()  # type: ignore
    logging.warning("Glossary Service: Failed to import app settings. Using fallback settings.")

logger = logging.getLogger(__name__)


class QueryEnrichmentService:
    def __init__(self, glossary_file_path: Optional[str] = None):
        actual_glossary_path = glossary_file_path if glossary_file_path is not None else settings.GLOSSARY_FILE_PATH

        self.glossary_data: List[Dict[str, str]] = []  # Stores dicts with 'term', 'display_term', 'definition'
        self._load_glossary(actual_glossary_path)
        logger.info(
            f"QueryEnrichmentService initialized with {len(self.glossary_data)} glossary entries from {actual_glossary_path}.")

    def _load_glossary(self, glossary_file_path: str):
        try:
            with open(glossary_file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f, delimiter='\t')
                for row in reader:
                    term = row.get('Termo Jurídico')
                    definition = row.get('Definição sintética')
                    if term and term.strip() and definition and definition.strip() and not term.strip().lower().startswith(
                            "=ai("):
                        self.glossary_data.append({
                            "term": term.strip().lower(),
                            "display_term": term.strip(),  # Keep original case for display
                            "definition": definition.strip()
                        })
            if not self.glossary_data:
                logger.warning(
                    f"No valid glossary entries loaded from {glossary_file_path}. Check file format and content (Termo Jurídico, Definição sintética).")
        except FileNotFoundError:
            logger.error(f"Glossary file not found for query enrichment: {glossary_file_path}")
        except Exception as e:
            logger.error(f"Error loading glossary for query enrichment from '{glossary_file_path}': {e}", exc_info=True)

    def _is_definition_in_query(self, definition: str, query_lower: str) -> bool:
        normalized_definition = re.sub(r'[.,;:?!]$', '', definition.lower()).strip()
        normalized_query = re.sub(r'[.,;:?!]$', '', query_lower).strip()

        if not normalized_definition:
            return False

        def_words = normalized_definition.split()
        if len(def_words) > 1:
            return re.search(r'\b' + re.escape(normalized_definition) + r'\b', normalized_query,
                             re.IGNORECASE) is not None
        else:  # For single word definitions, simple substring is okay, but could be made stricter
            return re.search(r'\b' + re.escape(normalized_definition) + r'\b', normalized_query,
                             re.IGNORECASE) is not None

    def enrich_query(self, query: str) -> str:
        if not self.glossary_data:
            return query

        original_query_lower = query.lower()
        enrichment_texts_set = set()

        for item in self.glossary_data:
            term_lower_for_match = item["term"]
            display_term = item["display_term"]

            if re.search(r'\b' + re.escape(term_lower_for_match) + r'\b', original_query_lower, re.IGNORECASE):
                if item["definition"] and not self._is_definition_in_query(item["definition"], original_query_lower):
                    if term_lower_for_match != item["definition"].lower():
                        enrichment_text = f"(contexto do termo '{display_term}': {item['definition']})"
                        enrichment_texts_set.add(enrichment_text)

        if enrichment_texts_set:
            logger.debug(
                f"Enriching query. Original: '{query}'. Added context: {' '.join(sorted(list(enrichment_texts_set)))}")
            return query + " " + " ".join(sorted(list(enrichment_texts_set)))
        return query


