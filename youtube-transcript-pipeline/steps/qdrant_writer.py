from zenml import step
from typing import List, Dict
import yaml
import logging
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct
import uuid

logger = logging.getLogger(__name__)

@step
def write_to_qdrant(
    chunks: List[Dict],
    config_path: str = "config/config.yaml"
) -> Dict:
    """Embedding'leri Qdrant'a yaz"""
    
    # Config yükle
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    qdrant_config = config['qdrant']
    
    # Qdrant client
    client = QdrantClient(
        host=qdrant_config['host'],
        port=qdrant_config['port']
    )
    
    collection_name = qdrant_config['collection_name']
    vector_size = qdrant_config['vector_size']
    
    try:
        # Collection var mı kontrol et, yoksa oluştur
        collections = client.get_collections().collections
        collection_exists = any(col.name == collection_name for col in collections)
        
        if not collection_exists:
            logger.info(f"Creating collection: {collection_name}")
            client.create_collection(
                collection_name=collection_name,
                vectors_config=VectorParams(
                    size=vector_size,
                    distance=Distance.COSINE
                )
            )
        else:
            logger.info(f"Collection already exists: {collection_name}")
            # Mevcut collection'ı temizlemek isterseniz:
            # client.delete_collection(collection_name=collection_name)
            # ve yeniden oluşturun
        
        # Points hazırla
        points = []
        for i, chunk in enumerate(chunks):
            # Embedding'i al ve kontrol et
            if 'embedding' not in chunk:
                logger.warning(f"No embedding for chunk {i}, skipping...")
                continue
            
            # Metadata hazırla (embedding hariç)
            payload = {
                'text': chunk['text'],
                'video_id': chunk['video_id'],
                'video_title': chunk['video_title'],
                'channel_id': chunk['channel_id'],
                'channel_title': chunk['channel_title'],
                'published_at': chunk['published_at'],
                'video_url': chunk['video_url'],
                'chunk_id': chunk.get('chunk_id', i),
                'total_chunks': chunk.get('total_chunks', 1)
            }
            
            # Point oluştur
            point = PointStruct(
                id=str(uuid.uuid4()),
                vector=chunk['embedding'],
                payload=payload
            )
            points.append(point)
        
        # Batch upload
        if points:
            batch_size = 100
            for i in range(0, len(points), batch_size):
                batch = points[i:i+batch_size]
                client.upsert(
                    collection_name=collection_name,
                    points=batch
                )
                logger.info(f"Uploaded batch {i//batch_size + 1}/{(len(points)-1)//batch_size + 1}")
            
            logger.info(f"Successfully uploaded {len(points)} points to Qdrant")
            
            # İstatistikleri topla
            collection_info = client.get_collection(collection_name)
            stats = {
                'total_vectors': collection_info.points_count,
                'vector_dimension': vector_size,
                'collection': collection_name,
                'new_vectors_added': len(points)
            }
        else:
            logger.warning("No points to upload to Qdrant")
            stats = {'total_vectors': 0}
        
        return stats
        
    except Exception as e:
        logger.error(f"Qdrant write error: {e}")
        raise