import re
from typing import Dict, List, Optional


class TextChunker:
    """Sequential chunker that preserves full coverage with word-aware boundaries."""

    BOUNDARY_CHARS = set(" \t\n\r.,!?;:—-…\"'()[]{}")

    def __init__(
        self,
        chunk_size: int = 1000,
        chunk_overlap: int = 200,
        min_chunk_length: int = 0,
    ) -> None:
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive.")

        self.chunk_size = chunk_size
        self.chunk_overlap = max(0, min(chunk_overlap, chunk_size - 1))
        self.min_chunk_length = max(0, min_chunk_length)

    @classmethod
    def _is_boundary(cls, char: str) -> bool:
        return char in cls.BOUNDARY_CHARS

    @staticmethod
    def _normalize_text(text: str) -> str:
        """Collapse multiple whitespace characters to a single space."""
        return re.sub(r"\s+", " ", text).strip()

    def _extend_end(self, text: str, end: int) -> int:
        """Extend chunk end until a boundary character is reached."""
        text_len = len(text)
        cursor = end
        while cursor < text_len and not self._is_boundary(text[cursor]):
            cursor += 1
        if cursor < text_len and self._is_boundary(text[cursor]):
            cursor += 1
        return min(cursor, text_len)

    def _rewind_start(self, text: str, start: int) -> int:
        """Move chunk start backward to the previous boundary."""
        cursor = start
        while cursor > 0 and not self._is_boundary(text[cursor - 1]):
            cursor -= 1
        while cursor < len(text) and text[cursor].isspace():
            cursor += 1
        return cursor

    def chunk_text(self, text: str, metadata: Optional[Dict] = None) -> List[Dict]:
        normalized = self._normalize_text(text)
        text_len = len(normalized)

        if text_len == 0:
            return []

        chunks: List[str] = []
        start = 0

        while start < text_len:
            initial_end = min(text_len, start + self.chunk_size)
            end = self._extend_end(normalized, initial_end)

            chunk_text = normalized[start:end].strip()
            if not chunk_text:
                break

            if len(chunk_text) < self.min_chunk_length and chunks:
                chunks[-1] = f"{chunks[-1]} {chunk_text}".strip()
            else:
                chunks.append(chunk_text)

            if end >= text_len:
                break

            next_start = max(0, end - self.chunk_overlap)
            next_start = self._rewind_start(normalized, next_start)
            if next_start <= start:
                next_start = min(text_len, end)
            start = next_start

        total_chunks = len(chunks)
        result: List[Dict] = []
        for chunk_id, chunk_text in enumerate(chunks):
            chunk_data = {
                "text": chunk_text,
                "chunk_id": chunk_id,
                "total_chunks": total_chunks,
            }
            if metadata:
                chunk_data.update(metadata)
            result.append(chunk_data)

        return result
