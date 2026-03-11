from sentence_transformers import SentenceTransformer
from config import EMBED_MODEL

model = SentenceTransformer(EMBED_MODEL)

def get_embeddings(chunks):
    return model.encode(chunks, convert_to_numpy=True)
