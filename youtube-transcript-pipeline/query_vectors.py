"""Qdrant'tan benzer vektörleri sorgula"""
from qdrant_client import QdrantClient
from sentence_transformers import SentenceTransformer
import yaml
import json

def search_similar(query_text: str, top_k: int = 5):
    """Benzer chunk'ları bul"""
    
    # Config yükle
    with open('config/config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    # Model yükle
    model = SentenceTransformer(config['embedding']['model_name'])
    
    # Query embedding oluştur
    query_embedding = model.encode(query_text).tolist()
    
    # Qdrant'a bağlan
    client = QdrantClient(
        host=config['qdrant']['host'],
        port=config['qdrant']['port']
    )
    
    # Benzer vektörleri ara
    results = client.search(
        collection_name=config['qdrant']['collection_name'],
        query_vector=query_embedding,
        limit=top_k
    )
    
    print(f"\n🔍 Sorgu: '{query_text}'")
    print("=" * 80)
    
    for i, result in enumerate(results, 1):
        print(f"\n📌 Sonuç {i} (Skor: {result.score:.4f})")
        print(f"Video: {result.payload['video_title']}")
        print(f"URL: {result.payload['video_url']}")
        print(f"Metin: {result.payload['text'][:200]}...")
        print("-" * 40)
    
    return results