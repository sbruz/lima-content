from __future__ import annotations

import pathlib
from functools import lru_cache
from typing import Dict, List, Optional

import yaml
from pydantic import BaseModel, Field, ValidationError, root_validator


class RetrySettings(BaseModel):
    attempts: int = Field(3, ge=1)
    delays_sec: List[int] = Field(default_factory=lambda: [5, 10])


class RateLimit(BaseModel):
    calls_per_minute: int = Field(..., gt=0)
    burst: int = Field(default=1, ge=1)


class LoggingSettings(BaseModel):
    level: str = Field("INFO")
    file: str = Field("./logs/last_run.log")


class ViewsSettings(BaseModel):
    base_min: int = Field(200, ge=0)
    base_max: int = Field(1000, ge=0)
    seed: Optional[int] = None


class AudioMixSettings(BaseModel):
    music_volume_db: float = Field(0.0)
    music_fade_in_ms: int = Field(0, ge=0)
    music_fade_out_ms: int = Field(0, ge=0)
    voice_fade_in_ms: int = Field(0, ge=0)
    voice_fade_out_ms: int = Field(0, ge=0)


class DatabaseRetrySettings(BaseModel):
    attempts: int = Field(3, ge=1)
    delays_sec: List[float] = Field(default_factory=lambda: [5.0])

    @root_validator(pre=True)
    def _ensure_delays(cls, values: Dict[str, List[float]]) -> Dict[str, List[float]]:
        delays = values.get("delays_sec")
        if not delays:
            values["delays_sec"] = [5.0]
        return values


class RangeSettings(BaseModel):
    categories: List[int] = Field(default_factory=lambda: [1, -1])
    subcategories: List[int] = Field(default_factory=lambda: [1, -1])
    positions: List[int] = Field(default_factory=lambda: [1, -1])

    @root_validator(pre=True)
    def _ensure_pairs(cls, values: Dict[str, List[int]]) -> Dict[str, List[int]]:
        for key, default in (
            ("categories", [1, -1]),
            ("subcategories", [1, -1]),
            ("positions", [1, -1]),
        ):
            candidate = values.get(key, default)
            if len(candidate) != 2:
                raise ValueError(f"{key} must contain exactly two integers")
            values[key] = candidate
        return values


class Config(BaseModel):
    steps: Dict[str, bool]
    range: RangeSettings
    languages: List[str]
    versions: List[str] = Field(default_factory=list)
    voices: List[int]
    affirmations_per_subcategory: int = Field(..., gt=0)
    regenerate_affirmations: bool = Field(True)
    music_prompt_tail_sec: float = Field(5.0, ge=0.0)
    threads: int = Field(4, gt=0)
    threads_audio: int = Field(10, gt=0)
    threads_music: int = Field(5, gt=0)
    retry: RetrySettings = Field(default_factory=RetrySettings)
    rate_limits: Dict[str, RateLimit] = Field(default_factory=dict)
    logging: LoggingSettings = Field(default_factory=LoggingSettings)
    ids: Dict[str, str] = Field(default_factory=dict)
    views: ViewsSettings = Field(default_factory=ViewsSettings)
    audio_mix: AudioMixSettings = Field(default_factory=AudioMixSettings)
    db_retry: DatabaseRetrySettings = Field(default_factory=DatabaseRetrySettings)
    script_affirmations_add_pauses: bool = Field(True)
    generate_audio_stub: bool = Field(False)

    @property
    def namespace(self) -> str:
        return self.ids.get("namespace", "lima-content")


def _read_yaml(path: pathlib.Path) -> Dict:
    with path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


@lru_cache()
def load_config(path: str | pathlib.Path = "config.yaml") -> Config:
    config_path = pathlib.Path(path).resolve()
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    raw = _read_yaml(config_path)
    try:
        return Config.model_validate(raw)
    except ValidationError as exc:
        raise RuntimeError(f"Invalid configuration: {exc}") from exc


__all__ = [
    "Config",
    "RateLimit",
    "RetrySettings",
    "LoggingSettings",
    "ViewsSettings",
    "AudioMixSettings",
    "RangeSettings",
    "load_config",
]
