"""Basit bir semantic search uygulaması"""
from qdrant_client import QdrantClient
from sentence_transformers import SentenceTransformer
import yaml
import gradio as gr

# Global değişkenler
model = None
client = None
config = None

def initialize():
    """Model ve client'ı başlat"""
    global model, client, config
    
    with open('config/config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    model = SentenceTransformer(config['embedding']['model_name'])
    client = QdrantClient(
        host=config['qdrant']['host'],
        port=config['qdrant']['port']
    )

def search(query: str, num_results: int = 5):
    """Semantic search yap"""
    if not query:
        return "Lütfen bir sorgu girin."
    
    # Embedding oluştur
    query_embedding = model.encode(query).tolist()
    
    # Ara
    results = client.search(
        collection_name=config['qdrant']['collection_name'],
        query_vector=query_embedding,
        limit=num_results
    )
    
    # Sonuçları formatla
    output = f"**Sorgu:** {query}\n\n"
    
    for i, result in enumerate(results, 1):
        output += f"### {i}. Sonuç (Benzerlik: {result.score:.3f})\n"
        output += f"**Video:** {result.payload['video_title']}\n"
        output += f"**Link:** {result.payload['video_url']}\n"
        output += f"**İçerik:** {result.payload['text'][:300]}...\n\n"
        output += "---\n\n"
    
    return output

# Gradio arayüzü
def create_interface():
    initialize()
    
    interface = gr.Interface(
        fn=search,
        inputs=[
            gr.Textbox(label="Arama Sorgusu", placeholder="Örn: stand-up gösterisi"),
            gr.Slider(1, 10, value=5, label="Sonuç Sayısı", step=1)
        ],
        outputs=gr.Markdown(label="Arama Sonuçları"),
        title="Cem Yılmaz Video Arama",
        description="YouTube videolarında semantic arama yapın",
        examples=[
            ["komik anılar", 5],
            ["çocukluk hikayeleri", 3],
            ["sinema", 5]
        ]
    )
    
    return interface

if __name__ == "__main__":
    # Gradio kullanmak için: pip install gradio
    app = create_interface()
    app.launch(share=False, server_port=7860)