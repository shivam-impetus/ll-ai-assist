import os
import pickle

import numpy as np

import config


def embedding_to_blob(embedding: np.ndarray) -> bytes:
    """Convert numpy array to binary blob for storage"""
    return embedding.astype(np.float32).tobytes()


def blob_to_embedding(blob: bytes, dim: int) -> np.ndarray:
    """Convert binary blob back to numpy array"""
    try:
        embedding = np.frombuffer(blob, dtype=np.float32).reshape(1, -1)
        return embedding
    except Exception as e:
        print(f"⚠ Error converting blob to embedding: {e}")
        # Return zero vector if conversion fails
        return np.zeros((1, dim), dtype=np.float32)


def load_or_create_embeddings():
    """Load cached embeddings if available"""
    if os.path.exists(config.EMBEDDINGS_CACHE) and os.path.exists(config.DOCS_CACHE):
        print(f"Loading cached embeddings from {config.EMBEDDINGS_CACHE}...")
        try:
            with open(config.EMBEDDINGS_CACHE, 'rb') as f:
                cached_data = pickle.load(f)
            with open(config.DOCS_CACHE, 'rb') as f:
                cached_docs = pickle.load(f)
            print(f"✓ Loaded {len(cached_docs)} cached document chunks")
            return cached_data, cached_docs
        except Exception as e:
            print(f"⚠ Error loading cache: {e}. Will rebuild.")

    return None, None
