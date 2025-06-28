"""
Unicode filtering utilities for safe text handling
"""

import re
import unicodedata

def clean_unicode_text(text):
    """Remove problematic Unicode characters from text"""
    if not isinstance(text, str):
        return text
    
    # Remove Unicode line/paragraph separators
    text = text.replace('\u2028', '\n')  # Line separator
    text = text.replace('\u2029', '\n\n')  # Paragraph separator
    
    # Remove other problematic characters
    # Keep only ASCII and common Unicode (Latin-1)
    cleaned = []
    for char in text:
        if ord(char) < 128:  # ASCII
            cleaned.append(char)
        elif ord(char) < 256:  # Latin-1
            cleaned.append(char)
        elif char in '\n\r\t':  # Allow common whitespace
            cleaned.append(char)
        else:
            # Replace with space or appropriate substitute
            if unicodedata.category(char) in ('Zs', 'Zl', 'Zp'):
                cleaned.append(' ')
            else:
                cleaned.append('?')
    
    return ''.join(cleaned)

def safe_print(text):
    """Safely print text with Unicode filtering"""
    return clean_unicode_text(text)