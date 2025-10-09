"""MongoDB uzerinden pipeline ciktilarini sorgula."""

import yaml
from pymongo import MongoClient


def query_videos() -> None:
    """List videos stored in MongoDB with basic statistics."""
    with open("config/config.yaml", "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)

    mongo_config = config["mongodb"]
    client = MongoClient(mongo_config["uri"])
    db = client[mongo_config["database"]]
    collection = db[mongo_config["collection"]]

    pipeline = [
        {
            "$group": {
                "_id": "$video_id",
                "title": {"$first": "$video_title"},
                "url": {"$first": "$video_url"},
                "chunk_count": {"$sum": 1},
            }
        },
        {"$sort": {"chunk_count": -1}},
    ]

    videos = list(collection.aggregate(pipeline))

    print(f"\nToplam {len(videos)} video bulundu:")
    print("=" * 80)

    for video in videos:
        print(f"- {video['title']}")
        print(f"  URL: {video['url']}")
        print(f"  Chunk sayisi: {video['chunk_count']}")
        print()

    total_chunks = collection.count_documents({})
    print("\nIstatistikler:")
    print(f"- Toplam video: {len(videos)}")
    print(f"- Toplam chunk: {total_chunks}")
    if videos:
        print(f"- Ortalama chunk/video: {total_chunks / len(videos):.1f}")

    client.close()


if __name__ == "__main__":
    query_videos()
