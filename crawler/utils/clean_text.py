import re
from html import unescape

def clean_text(text: str) -> str:
    """
    Cleans the input text by removing HTML tags and decoding HTML entities.
    """
    if not isinstance(text, str):
        return text
    # Remove HTML tags
    cleaned_text = re.sub(r'<[^>]*>', '', text)
    # Decode HTML entities
    cleaned_text = unescape(cleaned_text)
    return cleaned_text.strip()
