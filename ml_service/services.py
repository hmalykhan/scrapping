from sentence_transformers import SentenceTransformer
from typing import List

# 🔹 Load model ONCE (global singleton)
_model = None

def get_model() -> SentenceTransformer:
    global _model

    if _model is None:
        _model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

    return _model

def embed_text(text: str) -> List[float]:
    """
    Convert a single text string into a 384-dim embedding vector.
    """

    if not text or not text.strip():
        raise ValueError("Input text is empty")

    model = get_model()

    embedding = model.encode(text)

    # Convert numpy → python list (important for pgvector)
    return embedding.tolist()