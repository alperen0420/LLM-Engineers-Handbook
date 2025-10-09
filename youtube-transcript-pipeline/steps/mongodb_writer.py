from datetime import datetime
import logging
from typing import Dict, List

import yaml
from pymongo import MongoClient
from zenml import step

logger = logging.getLogger(__name__)


@step
def write_to_mongodb(
    chunks: List[Dict],
    config_path: str = "config/config.yaml",
) -> Dict:
    """Persist processed chunks into MongoDB."""

    with open(config_path, "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)

    mongo_config = config.get("mongodb", {})
    uri = mongo_config.get("uri", "mongodb://localhost:27017")
    database_name = mongo_config.get("database")
    collection_name = mongo_config.get("collection")

    if not database_name or not collection_name:
        raise ValueError("MongoDB database ve collection alanlari config dosyasinda bulunmali.")

    try:
        client = MongoClient(uri)
        db = client[database_name]
        collection = db[collection_name]
        logger.info("MongoDB baglantisi acildi: %s.%s", database_name, collection_name)

        existing_count = collection.count_documents({})
        if existing_count > 0:
            logger.info("Koleksiyonda %s mevcut dokuman var.", existing_count)

        now = datetime.utcnow()
        for chunk in chunks:
            chunk["created_at"] = now
            chunk["updated_at"] = now

        if chunks:
            result = collection.insert_many(chunks)
            inserted = len(result.inserted_ids)
            logger.info("%s dokuman MongoDB'ye yazildi.", inserted)
            stats = {
                "total_documents": inserted,
                "unique_videos": len({chunk["video_id"] for chunk in chunks if chunk.get("video_id")}),
                "collection": collection_name,
                "database": database_name,
            }
        else:
            logger.warning("MongoDB'ye yazilacak chunk bulunamadi.")
            stats = {"total_documents": 0}

        client.close()
        return stats

    except Exception as exc:  # pylint: disable=broad-except
        logger.error("MongoDB yazma hatasi: %s", exc)
        raise
