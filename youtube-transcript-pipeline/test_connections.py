"""Test database connections for the pipeline."""

import logging

import yaml
from pymongo import MongoClient
from qdrant_client import QdrantClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_mongodb() -> bool:
    """Check whether MongoDB is reachable."""
    with open("config/config.yaml", "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)

    mongo_config = config["mongodb"]

    try:
        client = MongoClient(mongo_config["uri"], serverSelectionTimeoutMS=5000)
        client.server_info()
        logger.info("MongoDB baglantisi basarili.")
        logger.info("Databases: %s", client.list_database_names())
        client.close()
        return True
    except Exception as exc:  # pylint: disable=broad-except
        logger.error("MongoDB baglantisi kurulamadi: %s", exc)
        return False


def test_qdrant() -> bool:
    """Check whether Qdrant is reachable."""
    with open("config/config.yaml", "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)

    qdrant_config = config["qdrant"]

    try:
        client = QdrantClient(
            host=qdrant_config["host"],
            port=qdrant_config["port"],
        )
        collections = client.get_collections()
        logger.info("Qdrant baglantisi basarili.")
        logger.info("Collections: %s", [c.name for c in collections.collections])
        return True
    except Exception as exc:  # pylint: disable=broad-except
        logger.error("Qdrant baglantisi kurulamadi: %s", exc)
        return False


if __name__ == "__main__":
    test_mongodb()
    test_qdrant()
