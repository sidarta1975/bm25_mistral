# python_backend_services/data_ingestion/tag_extractor.py
from typing import List, Dict, Set, Optional
import re
import logging

# Tenta importar settings para TAG_KEYWORDS_MAP
try:
    from python_backend_services.app.core.config import settings as app_settings
except ImportError:
    app_settings = None
    print(
        "tag_extractor.py: AVISO - Não foi possível importar 'settings' do projeto. Usando mapa de keywords padrão para teste.")

# Configurar logging básico se não configurado
if not logging.getLogger().hasHandlers():
    log_level = app_settings.LOG_LEVEL.upper() if app_settings and hasattr(app_settings, 'LOG_LEVEL') else "INFO"
    logging.basicConfig(level=log_level, format='%(asctime)s - %(levelname)s - [%(name)s:%(lineno)d] - %(message)s')
logger = logging.getLogger(__name__)


def extract_tags_from_filename(file_name: str) -> List[str]:
    """
    Extrai tags baseadas em palavras-chave no nome do arquivo.
    Exemplo: "peticao_alimentos_urgente.txt" -> ["alimentos", "urgente"]
    """
    tags: Set[str] = set()
    if not file_name: return []

    # Remove extensão, converte para minúsculas, substitui hífens/pontos por underscore, depois split por underscore
    name_part = file_name.lower()
    if '.' in name_part:
        name_part = name_part.rsplit('.', 1)[0]  # Remove a última extensão

    name_part = name_part.replace('-', '_').replace('.', '_')
    parts = name_part.split('_')

    # Lista de stopwords simples e comuns em nomes de arquivo
    common_stopwords = [
        "peticao", "acao", "de", "do", "da", "o", "a", "e", "para", "com",
        "modelo", "inicial", "final", "peca", "docx", "doc", "pdf", "txt",
        "dr", "dra", "exmo", "sr", "sra", "no", "na", "em", "dos", "das",
        "rev", "atualizado", "finalizado", "versao", "v1", "v2", "v3"
    ]

    for part in parts:
        part_clean = part.strip()
        if part_clean and part_clean not in common_stopwords and len(part_clean) > 2:
            # Tenta remover apenas números no final, se houver (ex: "alimentos1" -> "alimentos")
            part_clean = re.sub(r'\d+$', '', part_clean)
            if part_clean and len(part_clean) > 2:  # Verifica de novo após remover números
                tags.add(part_clean)
    return sorted(list(tags))


def extract_tags_from_content_keywords(
        document_content: str,
        tag_keyword_map: Dict[str, List[str]]  # Ex: {"palavra_chave_no_texto": ["tag1", "tag_associada"]}
) -> List[str]:
    """
    Extrai tags procurando por palavras-chave predefinidas no conteúdo do documento.
    O tag_keyword_map mapeia uma palavra-chave encontrada (minúscula) para uma ou mais tags.
    """
    found_tags: Set[str] = set()
    if not document_content or not tag_keyword_map:
        return []

    content_lower = document_content.lower()

    for keyword, associated_tags in tag_keyword_map.items():
        # Usar regex para encontrar a palavra-chave como palavra inteira (word boundary)
        # Escapar a keyword para o regex
        keyword_escaped = re.escape(keyword.lower())
        pattern = r'\b' + keyword_escaped + r'\b'
        if re.search(pattern, content_lower):
            for tag in associated_tags:
                found_tags.add(tag.lower().strip())

    return sorted(list(found_tags))


def process_document_for_tags(
        document_data: Dict[str, Any],  # Espera um dict com 'file_name' e 'full_text_content'
        tag_keyword_map: Optional[Dict[str, List[str]]] = None
) -> Dict[str, Any]:
    """
    Processa um único documento para extrair tags do nome do arquivo e do conteúdo.
    Adiciona uma chave 'extracted_tags' ao dicionário do documento.
    """
    all_extracted_tags: Set[str] = set()

    file_name = document_data.get("file_name")
    if file_name:
        tags_from_name = extract_tags_from_filename(file_name)
        for tag in tags_from_name:
            all_extracted_tags.add(tag)

    content = document_data.get("full_text_content")
    if content and tag_keyword_map:
        tags_from_content = extract_tags_from_content_keywords(content, tag_keyword_map)
        for tag in tags_from_content:
            all_extracted_tags.add(tag)

    document_data["extracted_tags"] = sorted(list(all_extracted_tags))
    return document_data


def get_default_tag_keyword_map() -> Dict[str, List[str]]:
    """ Retorna um mapa de palavras-chave para tags padrão. """
    # Este mapa deve ser mais extenso e refinado. Pode vir de um arquivo de config.
    return {
        "alimentos": ["direito de família", "pensão alimentícia", "fixação de alimentos"],
        "pensão alimentícia": ["direito de família", "pensão alimentícia", "execução de alimentos"],
        "divórcio": ["direito de família", "dissolução de casamento", "partilha de bens"],
        "divórcio litigioso": ["direito de família", "dissolução de casamento", "divórcio"],
        "guarda compartilhada": ["direito de família", "guarda de filhos", "poder familiar"],
        "usucapião": ["direito civil", "propriedade", "direitos reais", "regularização fundiária"],
        "usucapião extraordinário": ["direito civil", "propriedade", "usucapião"],
        "contrato de aluguel": ["direito civil", "contratos", "locação residencial", "locação comercial"],
        "locação": ["direito civil", "contratos", "locação"],
        "reintegração de posse": ["direito civil", "posse", "direitos reais"],
        "dano moral": ["responsabilidade civil", "indenização", "danos"],
        "danos morais": ["responsabilidade civil", "indenização", "danos"],  # Variação
        "inventário": ["direito das sucessões", "herança", "partilha"],
        "testamento": ["direito das sucessões", "herança", "planejamento sucessório"],
        "busca e apreensão": ["processual", "medida cautelar"],
        "habeas corpus": ["direito penal", "processo penal", "liberdade"],
        "mandado de segurança": ["direito constitucional", "direito administrativo", "remédio constitucional"],
        # Adicione mais termos e suas tags associadas
    }


# Exemplo de como usar em um pipeline (para ser chamado pelo MetadataEnricher, por exemplo)
def enrich_documents_with_tags_batch(
        documents_batch: List[Dict[str, Any]],
        tag_keyword_map: Optional[Dict[str, List[str]]] = None
) -> List[Dict[str, Any]]:
    """
    Adiciona tags a um lote de documentos.
    Esta função seria chamada após o parsing inicial dos documentos.
    """
    if tag_keyword_map is None:
        if app_settings and hasattr(app_settings, 'TAG_KEYWORDS_MAP') and app_settings.TAG_KEYWORDS_MAP:
            tag_keyword_map = app_settings.TAG_KEYWORDS_MAP
            logger.info("Usando TAG_KEYWORDS_MAP de settings para extração de tags.")
        else:
            tag_keyword_map = get_default_tag_keyword_map()
            logger.info("Usando mapa de keywords padrão para extração de tags.")

    processed_batch = []
    for doc_data in documents_batch:
        processed_batch.append(process_document_for_tags(doc_data, tag_keyword_map))
    return processed_batch


if __name__ == '__main__':
    logger.info("--- Testando Tag Extractor Standalone ---")

    # Usa o mapa padrão ou de settings se disponível
    if app_settings and hasattr(app_settings, 'TAG_KEYWORDS_MAP') and app_settings.TAG_KEYWORDS_MAP:
        keyword_map_for_test = app_settings.TAG_KEYWORDS_MAP
        logger.info("Usando TAG_KEYWORDS_MAP de settings para o teste.")
    else:
        keyword_map_for_test = get_default_tag_keyword_map()
        logger.info("Usando mapa de keywords padrão para o teste.")

    example_docs_for_tagging_test = [
        {
            "document_id": "docA",
            "file_name": "peticao_alimentos_urgente_v1.txt",
            "full_text_content": "Esta é uma petição de alimentos para o menor J.S. Discute o direito de família e a necessidade de pensão alimentícia. Requer a fixação de alimentos provisórios."
        },
        {
            "document_id": "docB",
            "file_name": "modelo_contrato_locacao_residencial_2024.docx",  # Teste com .docx no nome
            "full_text_content": "Segue modelo de contrato de aluguel para fins residenciais. Envolve direito civil e obrigações contratuais de locação."
        },
        {
            "document_id": "docC",
            "file_name": "Defesa_Acao_Reintegracao_Posse_Fazenda.pdf",
            "full_text_content": "Defesa em ação de reintegração de posse de imóvel rural. Alega-se usucapião extraordinário como matéria de defesa. A posse é antiga."
        },
        {
            "document_id": "docD",
            "file_name": "MS_contra_ato_adm.txt",
            "full_text_content": "Impetração de Mandado de Segurança contra ato coator de autoridade administrativa."
        }
    ]

    logger.info("\n--- Testando process_document_for_tags ---")
    for doc_example in example_docs_for_tagging_test:
        tagged_doc = process_document_for_tags(doc_example, keyword_map_for_test)
        print(f"Documento: {tagged_doc.get('file_name')}")
        print(f"  Tags Extraídas: {tagged_doc.get('extracted_tags')}")

    logger.info("\n--- Testando enrich_documents_with_tags_batch ---")
    batch_result = enrich_documents_with_tags_batch(example_docs_for_tagging_test, keyword_map_for_test)
    for doc_res in batch_result:
        print(f"Batch - Documento: {doc_res.get('file_name')}, Tags: {doc_res.get('extracted_tags')}")

    logger.info("--- Teste Standalone do Tag Extractor Concluído ---")