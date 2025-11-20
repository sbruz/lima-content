from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Type

import httpx
from pydantic import BaseModel, create_model

from src.config import Config
from src.runtime import (
    FatalStepError,
    Job,
    RetryableStepError,
    filter_by_range,
    logger,
    make_job,
)
from src.steps.base import BaseStep
from src.utils.supabase_client import get_supabase_client
from src.utils.llm_client import LLMClient


@dataclass(frozen=True)
class CategoryJobPayload:
    category_id: int
    name: str


class Step1LocalizeCategories(BaseStep):
    NAME = "localize_categories"
    PROMPT_PATH = Path(__file__).resolve().parents[2] / "docs" / "agents" / "category.md"

    def __init__(self, config: Config) -> None:
        super().__init__(config)
        self.supabase = get_supabase_client()
        self.llm_model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
        self.llm_temperature = float(os.getenv("OPENAI_TEMPERATURE", "0.3"))
        self.llm_client = LLMClient()
        self._prompt_cache: Optional[str] = None
        self._schema_model: Optional[Type[BaseModel]] = None

    def _prompt(self) -> str:
        if self._prompt_cache is None:
            try:
                self._prompt_cache = self.PROMPT_PATH.read_text(encoding="utf-8")
            except FileNotFoundError as exc:
                raise FatalStepError(f"Prompt file not found: {self.PROMPT_PATH}") from exc
        return self._prompt_cache

    def _acquire_limit(self, bucket: str) -> None:
        limiter = getattr(self, "rate_limiter", None)
        if limiter is not None:
            limiter.acquire(bucket)

    def load_jobs(self) -> List[Job]:
        if not self.should_run():
            return []

        categories = self._fetch_categories()
        filtered = filter_by_range(categories, tuple(self.config.range.categories))
        return [
            make_job(
                self.NAME,
                {"category_id": item["id"], "name": item["name"]},
                key_fields=["category_id"],
            )
            for item in filtered
        ]

    def process(self, job: Job) -> None:
        payload = CategoryJobPayload(
            category_id=job.payload["category_id"],
            name=job.payload.get("name") or "",
        )
        if not payload.name:
            raise FatalStepError(f"Category {payload.category_id} has empty name")

        logger.info(
            "[BUSINESS] Start category localization",
            category_id=payload.category_id,
            name=payload.name,
        )

        started_at = time.perf_counter()
        try:
            localization = self._generate_localization(payload.name)
        except (httpx.HTTPError, ValueError, KeyError, json.JSONDecodeError) as exc:
            raise RetryableStepError(f"LLM localization failed: {exc}") from exc

        self._acquire_limit("supabase")
        try:
            (
                self.supabase.table("categories")
                .update({"localization": localization})
                .eq("id", payload.category_id)
                .execute()
            )
        except Exception as exc:  # pragma: no cover - supabase client handles errors
            raise RetryableStepError(f"Supabase update failed: {exc}") from exc

        duration_ms = (time.perf_counter() - started_at) * 1000
        logger.info(
            "[BUSINESS] Category localized",
            category_id=payload.category_id,
            duration_ms=round(duration_ms, 2),
            languages=list(localization.keys()),
        )

    def _fetch_categories(self) -> List[dict[str, Any]]:
        self._acquire_limit("supabase")
        try:
            response = (
                self.supabase.table("categories")
                .select("id, name, position")
                .order("position")
                .execute()
            )
        except Exception as exc:
            raise FatalStepError(f"Failed to load categories: {exc}") from exc
        data = getattr(response, "data", []) or []
        return data

    def _generate_localization(self, category_name: str) -> Dict[str, str]:
        self._acquire_limit("openai")
        messages = [
            {"role": "system", "content": self._prompt()},
            {
                "role": "user",
                "content": json.dumps({"category_name": category_name}, ensure_ascii=False),
            },
        ]
        response = self.llm_client.chat(
            messages,
            model=self.llm_model,
            temperature=self.llm_temperature,
            response_schema=self._build_response_schema(),
        )
        choices = response.get("choices") or []
        if not choices:
            raise ValueError("Empty response choices")
        message_block = choices[0].get("message") or {}
        content = message_block.get("content")
        if not content:
            raise ValueError("Empty response content")

        parsed = json.loads(content)
        localization = {
            lang: parsed.get(lang, "").strip()
            for lang in self.config.languages
        }
        missing = [lang for lang, value in localization.items() if not value]
        if missing:
            raise ValueError(f"Missing localization for languages: {missing}")
        return localization

    def _build_response_schema(self) -> Type[BaseModel]:
        if self._schema_model is None:
            fields = {lang: (str, ...) for lang in self.config.languages}
            self._schema_model = create_model(  # type: ignore[misc]
                "CategoryLocalizationModel",
                **fields,
            )
        return self._schema_model


__all__ = ["Step1LocalizeCategories"]
