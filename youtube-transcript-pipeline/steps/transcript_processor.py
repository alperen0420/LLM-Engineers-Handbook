from zenml import step
from typing import List, Dict
import yaml
import logging
from utils.text_chunker import TextChunker

logger = logging.getLogger(__name__)

@step(enable_cache=False)
def process_transcripts(
    videos: List[Dict],
    config_path: str = "config/config.yaml"
) -> List[Dict]:
    """Transkriptleri işle ve chunk'lara böl"""
    
    # Config yükle
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    chunk_config = config['text_processing']
    logger.info(
        "Chunk settings -> size: %s, overlap: %s, min_length: %s",
        chunk_config['chunk_size'],
        chunk_config['chunk_overlap'],
        chunk_config.get('min_chunk_length', 0),
    )

    chunker = TextChunker(
        chunk_size=chunk_config['chunk_size'],
        chunk_overlap=chunk_config['chunk_overlap'],
        min_chunk_length=chunk_config.get('min_chunk_length', 0)
    )
    
    all_chunks = []
    
    for video in videos:
        if not video.get('transcript'):
            continue
        
        logger.info(f"Processing chunks for: {video['title']}")
        
        # Video metadata
        metadata = {
            'video_id': video['video_id'],
            'video_title': video['title'],
            'channel_id': video['channel_id'],
            'channel_title': video['channel_title'],
            'published_at': video['published_at'],
            'video_url': f"https://www.youtube.com/watch?v={video['video_id']}"
        }
        
        # Chunk'lara böl
        chunks = chunker.chunk_text(video['transcript'], metadata)
        
        all_chunks.extend(chunks)
        logger.info(f"Created {len(chunks)} chunks for video: {video['title']}")
    
    logger.info(f"Total chunks created: {len(all_chunks)}")
    return all_chunks
