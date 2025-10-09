import logging
from pathlib import Path
from pipelines.youtube_transcript_pipeline import youtube_transcript_pipeline
from zenml.logger import get_logger

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = get_logger(__name__)


def check_config_files() -> None:
    """Validate that the configuration file exists and contains required fields."""
    config_file = Path("config/config.yaml")

    if not config_file.exists():
        raise FileNotFoundError("config/config.yaml dosyasi bulunamadi!")

    import yaml

    with open(config_file, "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)

    video_urls = config.get("youtube", {}).get("video_urls") or []
    if not video_urls:
        raise ValueError(
            "config/config.yaml icindeki youtube.video_urls alanina islenecek video baglantilarini ekleyin."
        )

    logger.info("Config dosyasi hazir")


def check_services() -> None:
    """Check that MongoDB and Qdrant services are running in Docker."""
    import subprocess

    try:
        result = subprocess.run(
            ["docker", "ps", "--format", "table {{.Names}}"],
            capture_output=True,
            text=True,
            check=False,
        )
        running_containers = result.stdout.lower()

        if "mongo" not in running_containers:
            logger.warning("MongoDB container calismiyor olabilir!")
        else:
            logger.info("MongoDB container calisiyor")

        if "qdrant" not in running_containers:
            logger.warning("Qdrant container calismiyor olabilir!")
        else:
            logger.info("Qdrant container calisiyor")

    except Exception as exc:  # pylint: disable=broad-except
        logger.warning("Docker kontrolu yapilamadi: %s", exc)


def main() -> None:
    """Run the YouTube transcript pipeline."""
    logger.info("=" * 50)
    logger.info("YouTube Transcript Pipeline Baslatiliyor")
    logger.info("=" * 50)

    try:
        check_config_files()
        check_services()
    except Exception as exc:  # pylint: disable=broad-except
        logger.error("On kontrol hatasi: %s", exc)
        return

    try:
        logger.info("\nPipeline baslatiliyor...")
        pipe = youtube_transcript_pipeline()

        logger.info("\nPipeline basariyla tamamlandi!")
        logger.info("Run ID: %s", pipe.id)

        logger.info("\nDetaylar icin ZenML Dashboard:")
        logger.info("http://localhost:8237")

    except Exception as exc:  # pylint: disable=broad-except
        logger.error("\nPipeline hatasi: %s", exc)
        raise


if __name__ == "__main__":
    main()
