import json
import logging
from typing import Dict, Iterable, List, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, quote_plus, urlparse
from urllib.request import Request, urlopen

from pytube import YouTube
from pytube.exceptions import PytubeError
from youtube_transcript_api import YouTubeTranscriptApi  # type: ignore
from youtube_transcript_api._errors import (  # type: ignore
    NoTranscriptFound,
    TranscriptsDisabled,
    VideoUnavailable,
)

logger = logging.getLogger(__name__)


class YouTubeHelper:
    """Utility helpers to work with YouTube videos and transcripts."""

    def __init__(self, preferred_languages: Optional[List[str]] = None) -> None:
        self.preferred_languages = preferred_languages or ["tr", "en"]
        self._transcript_api = YouTubeTranscriptApi()

    @staticmethod
    def build_video_url(video_id: str) -> str:
        """Return canonical YouTube watch URL."""
        return f"https://www.youtube.com/watch?v={video_id}"

    @staticmethod
    def extract_video_id(video_url: str) -> Optional[str]:
        """Extract the video ID from any supported YouTube URL."""
        parsed = urlparse(video_url)
        host = (parsed.hostname or "").lower().lstrip("www.")

        if host in {"youtu.be"}:
            video_id = parsed.path.lstrip("/")
            return video_id or None

        if host in {"youtube.com", "m.youtube.com"}:
            query_params = parse_qs(parsed.query)
            if "v" in query_params and query_params["v"]:
                return query_params["v"][0]

            # Handle /embed/<id> or /shorts/<id>
            parts = [part for part in parsed.path.split("/") if part]
            if parts:
                last_part = parts[-1]
                if last_part.startswith("watchv="):
                    return last_part.replace("watchv=", "", 1)
                return last_part

        return None

    @staticmethod
    def _join_transcript(entries: Iterable) -> str:
        """Join transcript entries into a single string."""
        parts: List[str] = []
        for entry in entries:
            text = None
            if hasattr(entry, "text"):
                text = getattr(entry, "text")
            elif isinstance(entry, dict):
                text = entry.get("text")

            if not text:
                continue

            cleaned = str(text).strip()
            if cleaned:
                parts.append(cleaned)

        return " ".join(parts)

    def _fallback_metadata(self, video_id: str) -> Dict:
        """Return minimal metadata when rich information cannot be fetched."""
        return {
            "video_id": video_id,
            "title": video_id,
            "description": "",
            "published_at": None,
            "channel_id": None,
            "channel_title": None,
            "video_url": self.build_video_url(video_id),
        }

    def _fetch_oembed_metadata(self, video_id: str) -> Optional[Dict]:
        """Retrieve basic metadata via YouTube's oEmbed endpoint."""
        oembed_url = (
            "https://www.youtube.com/oembed"
            f"?format=json&url={quote_plus(self.build_video_url(video_id))}"
        )
        request = Request(
            oembed_url,
            headers={"User-Agent": "Mozilla/5.0 (youtube-transcript-pipeline)"},
        )
        try:
            with urlopen(request, timeout=10) as response:  # nosec
                payload = json.loads(response.read().decode("utf-8"))
        except (HTTPError, URLError, TimeoutError, ValueError, json.JSONDecodeError) as exc:
            logger.debug("oEmbed metadata getirilemedi (%s): %s", video_id, exc)
            return None

        return {
            "video_id": video_id,
            "title": payload.get("title", video_id),
            "description": "",
            "published_at": None,
            "channel_id": None,
            "channel_title": payload.get("author_name"),
            "video_url": self.build_video_url(video_id),
        }

    def get_video_metadata(self, video_url: str) -> Optional[Dict]:
        """Fetch basic metadata for a given YouTube URL."""
        video_id = self.extract_video_id(video_url)
        if not video_id:
            logger.error("Video ID parse edilemedi: %s", video_url)
            return None

        canonical_url = self.build_video_url(video_id)

        try:
            yt = YouTube(canonical_url)
            title = yt.title
            description = yt.description or ""
            publish_date = yt.publish_date.isoformat() if yt.publish_date else None
            channel_id = getattr(yt, "channel_id", None)
            channel_title = getattr(yt, "author", None)
            return {
                "video_id": video_id,
                "title": title,
                "description": description,
                "published_at": publish_date,
                "channel_id": channel_id,
                "channel_title": channel_title,
                "video_url": canonical_url,
            }
        except PytubeError as exc:
            logger.info(
                "Pytube oEmbed'e düştü (%s): %s",
                video_id,
                exc,
            )
        except Exception as exc:  # pylint: disable=broad-except
            logger.info(
                "Pytube beklenmeyen hata nedeniyle oEmbed denenecek (%s): %s",
                video_id,
                exc,
            )

        oembed_metadata = self._fetch_oembed_metadata(video_id)
        if oembed_metadata:
            logger.info("oEmbed metadata kullanıldı (%s)", video_id)
            return oembed_metadata

        logger.warning(
            "Video metadata bulunamadı, minimal metadata kullanilacak (%s)",
            video_id,
        )
        return self._fallback_metadata(video_id)

    def get_transcript(self, video_id: str) -> Optional[str]:
        """Fetch the transcript text for a given video ID."""
        try:
            transcript = self._transcript_api.fetch(
                video_id, languages=self.preferred_languages
            )
            return self._join_transcript(transcript)
        except NoTranscriptFound:
            pass  # fallback to manual search below
        except (TranscriptsDisabled, NoTranscriptFound, VideoUnavailable) as exc:
            logger.warning("Video icin transcript devre disi (%s): %s", video_id, exc)
            return None
        except Exception as exc:  # pylint: disable=broad-except
            logger.error("Transcript listesi cekilemedi (%s): %s", video_id, exc)
            return None

        try:
            transcript_list = self._transcript_api.list(video_id)
        except (TranscriptsDisabled, NoTranscriptFound, VideoUnavailable) as exc:
            logger.warning("Video icin transcript devre disi (%s): %s", video_id, exc)
            return None
        except Exception as exc:  # pylint: disable=broad-except
            logger.error("Transcript listesi cekilemedi (%s): %s", video_id, exc)
            return None

        for language in self.preferred_languages:
            try:
                candidate = transcript_list.find_transcript([language])
                return self._join_transcript(candidate.fetch())
            except NoTranscriptFound:
                try:
                    generated = transcript_list.find_generated_transcript([language])
                    return self._join_transcript(generated.fetch())
                except NoTranscriptFound:
                    continue

        for transcript in transcript_list:
            try:
                return self._join_transcript(transcript.fetch())
            except Exception as exc:  # pylint: disable=broad-except
                logger.debug(
                    "Transcript fetch denemesi basarisiz (%s - %s): %s",
                    video_id,
                    transcript.language_code,
                    exc,
                )
                continue

        logger.warning("Transcript bulunamadi (%s)", video_id)
        return None
