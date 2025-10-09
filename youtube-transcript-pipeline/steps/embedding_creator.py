from zenml import step
from typing import List, Dict
import yaml
import logging
import numpy as np
from sentence_transformers import SentenceTransformer
import torch

logger = logging.getLogger(__name__)

@step
def create_embeddings(
    chunks: List[Dict],
    config_path: str = "config/config.yaml"
) -> List[Dict]:
    """Chunk'lar için embedding oluştur"""
    
    # Config yükle
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    embed_config = config['embedding']
    
    # Model yükle
    logger.info(f"Loading model: {embed_config['model_name']}")
    device = embed_config.get('device', 'cpu')
    
    if device == 'cuda' and not torch.cuda.is_available():
        device = 'cpu'
        logger.warning("CUDA not available, using CPU")
    
    model = SentenceTransformer(embed_config['model_name'], device=device)
    
    # Batch processing için hazırlık
    batch_size = embed_config.get('batch_size', 32)
    texts = [chunk['text'] for chunk in chunks]
    
    logger.info(f"Creating embeddings for {len(texts)} chunks...")
    
    # Embedding oluştur
    embeddings = []
    for i in range(0, len(texts), batch_size):
        batch = texts[i:i+batch_size]
        batch_embeddings = model.encode(
            batch,
            convert_to_tensor=False,
            show_progress_bar=True
        )
        embeddings.extend(batch_embeddings)
        logger.info(f"Processed batch {i//batch_size + 1}/{(len(texts)-1)//batch_size + 1}")
    
    # Chunk'lara embedding ekle
    for chunk, embedding in zip(chunks, embeddings):
        chunk['embedding'] = embedding.tolist()
    
    logger.info(f"Successfully created {len(embeddings)} embeddings")
    return chunks