import unicodedata
import re

def normalize_text(text):
    text = text.lower()
    text = unicodedata.normalize('NFD', text)
    text = re.sub(r'[\u0300-\u036f]', '', text)  # Remove accents
    text = re.sub(r'\s+', ' ', text)  # Replace multiple spaces with one
    text = text.strip()
    return text
