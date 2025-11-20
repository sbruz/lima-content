from __future__ import annotations

import os
from typing import Any, Dict, Optional

import httpx
from dotenv import load_dotenv

load_dotenv()


class ElevenLabsClient:
    """Minimal ElevenLabs TTS client wrapper."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: str = "https://api.elevenlabs.io/v1",
        timeout: float = 120.0,
    ) -> None:
        self.api_key = api_key or os.getenv("ELEVENLABS_API_KEY")
        if not self.api_key:
            raise RuntimeError("ELEVENLABS_API_KEY is not configured")
        self.base_url = base_url.rstrip("/")
        self._client = httpx.Client(
            base_url=self.base_url,
            headers={"xi-api-key": self.api_key},
            timeout=timeout,
        )

    def generate_speech(self, voice_id: str, payload: Dict[str, Any]) -> bytes:
        response = self._client.post(f"/text-to-speech/{voice_id}", json=payload)
        response.raise_for_status()
        return response.content

    def close(self) -> None:
        self._client.close()


__all__ = ["ElevenLabsClient"]
