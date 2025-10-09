"""Show simple statistics for the pipeline outputs."""

import yaml
from pymongo import MongoClient
from qdrant_client import QdrantClient
from rich.console import Console
from rich.table import Table

console = Console()


def get_stats() -> None:
    """Display aggregated statistics about MongoDB and Qdrant states."""
    with open("config/config.yaml", "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)

    mongo_config = config["mongodb"]
    mongo_client = MongoClient(mongo_config["uri"])
    db = mongo_client[mongo_config["database"]]
    collection = db[mongo_config["collection"]]

    mongo_count = collection.count_documents({})
    unique_videos = collection.distinct("video_id")

    qdrant_client = QdrantClient(
        host=config["qdrant"]["host"],
        port=config["qdrant"]["port"],
    )

    try:
        collection_info = qdrant_client.get_collection(config["qdrant"]["collection_name"])
        qdrant_count = collection_info.points_count
    except Exception:  # pylint: disable=broad-except
        qdrant_count = 0

    table = Table(title="Pipeline Istatistikleri")
    table.add_column("Metrik", style="cyan", no_wrap=True)
    table.add_column("Deger", style="magenta")

    table.add_row("MongoDB Dokumanlari", str(mongo_count))
    table.add_row("Benzersiz Videolar", str(len(unique_videos)))
    table.add_row("Qdrant Vektorleri", str(qdrant_count))
    table.add_row("Embedding Boyutu", str(config["qdrant"]["vector_size"]))
    console.print(table)

    pipeline = [
        {"$group": {"_id": "$video_title", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 5},
    ]

    top_videos = list(collection.aggregate(pipeline))

    video_table = Table(title="En cok Chunk Iceren Videolar")
    video_table.add_column("Video", style="cyan")
    video_table.add_column("Chunk Sayisi", style="magenta")

    for video in top_videos:
        video_table.add_row((video["_id"] or "")[:50], str(video["count"]))

    console.print(video_table)

    mongo_client.close()


if __name__ == "__main__":
    get_stats()
