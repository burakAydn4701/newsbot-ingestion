import re

def chunk_text(text, chunk_size=600):
    """
    Splits text into chunks of ~chunk_size characters, respecting sentence boundaries.
    
    Args:
        text (str): The article content (HTML should be cleaned first).
        chunk_size (int): Approximate size of each chunk in characters.
    
    Returns:
        List[str]: List of text chunks.
    """
    # Split into sentences
    sentences = re.split(r'(?<=[.!?]) +', text)
    
    chunks = []
    current_chunk = ""
    
    for sentence in sentences:
        # If adding this sentence exceeds chunk_size, save current chunk and start a new one
        if len(current_chunk) + len(sentence) > chunk_size:
            if current_chunk:  # avoid empty chunk
                chunks.append(current_chunk.strip())
            current_chunk = sentence
        else:
            current_chunk += " " + sentence
    
    # Append any remaining text
    if current_chunk:
        chunks.append(current_chunk.strip())
    
    return chunks