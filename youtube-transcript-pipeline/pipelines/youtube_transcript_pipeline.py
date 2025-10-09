from zenml import pipeline
from steps.youtube_fetcher import fetch_youtube_videos
from steps.transcript_processor import process_transcripts
from steps.embedding_creator import create_embeddings
from steps.mongodb_writer import write_to_mongodb
from steps.qdrant_writer import write_to_qdrant
import logging

logger = logging.getLogger(__name__)

@pipeline(
    name="youtube_transcript_pipeline",
    enable_cache=False  # Her çalıştırmada fresh output üret
)
def youtube_transcript_pipeline():
    """
    YouTube videolarından transkript çekip vektör veritabanına yazan pipeline
    """
    
    # Step 1: YouTube'dan videoları ve transkriptleri çek
    videos = fetch_youtube_videos()
    
    # Step 2: Transkriptleri işle ve chunk'lara böl
    chunks = process_transcripts(videos)
    
    # Step 3: Her chunk için embedding oluştur
    chunks_with_embeddings = create_embeddings(chunks)
    
    # Step 4: MongoDB'ye yaz
    mongo_stats = write_to_mongodb(chunks_with_embeddings)
    
    # Step 5: Qdrant'a yaz
    qdrant_stats = write_to_qdrant(
        chunks_with_embeddings,
        after=["write_to_mongodb"]  # MongoDB'den sonra çalışsın
    )
    
    return mongo_stats, qdrant_stats
