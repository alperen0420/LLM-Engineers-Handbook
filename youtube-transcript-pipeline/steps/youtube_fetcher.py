from typing import Dict, List
import logging
import yaml
from zenml import step

from utils.youtube_helper import YouTubeHelper

logger = logging.getLogger(__name__)


@step
def fetch_youtube_videos(
    config_path: str = "config/config.yaml",
) -> List[Dict]:
    """Fetch transcripts for the configured YouTube video URLs."""

    with open(config_path, "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)

    video_urls = config.get("youtube", {}).get("video_urls") or []
    if not video_urls:
        raise ValueError(
            "Config dosyasindaki youtube.video_urls alanina en az bir video baglantisi ekleyin."
        )

    helper = YouTubeHelper()
    videos_with_transcripts: List[Dict] = []

    for url in video_urls:
        logger.info("Processing video: %s", url)

        metadata = helper.get_video_metadata(url)
        if not metadata:
            logger.warning("Video bilgileri getirilemedi: %s", url)
            continue

        metadata.setdefault("video_url", helper.build_video_url(metadata["video_id"]))

        transcript = helper.get_transcript(metadata["video_id"])
        if not transcript:
            logger.warning("Transcript bulunamadi: %s", url)
            continue

        metadata["transcript"] = transcript
        metadata["has_transcript"] = True
        videos_with_transcripts.append(metadata)
        logger.info("Transcript basariyla alindi: %s", metadata["title"])

    logger.info(
        "Toplam %s videonun transcripti cekildi.", len(videos_with_transcripts)
    )
    return videos_with_transcripts
