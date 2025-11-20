from __future__ import annotations

import json
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
from src.utils.llm_client import LLMClient
from src.utils.supabase_client import get_supabase_client


@dataclass(frozen=True)
class SubcategoryBatch:
    category_id: int
    category_name: str
    subcategories: List[Dict[str, Any]]


class Step2LocalizeSubcategories(BaseStep):
    NAME = "localize_subcategories"
    PROMPT_PATH = (
        Path(__file__).resolve().parents[2] / "docs" / "agents" / "subcategory.md"
    )

    def __init__(self, config: Config) -> None:
        super().__init__(config)
        self.supabase = get_supabase_client()
        self.llm_client = LLMClient()
        self.llm_model = self.config.ids.get("subcategory_model") or self.config.ids.get(
            "category_model", "gpt-5"
        )
        self._prompt_cache: Optional[str] = None
        self._response_schema: Optional[Type[BaseModel]] = None

    def _prompt(self) -> str:
        if self._prompt_cache is None:
            try:
                self._prompt_cache = self.PROMPT_PATH.read_text(encoding="utf-8")
            except FileNotFoundError as exc:
                raise FatalStepError(f"Prompt file not found: {self.PROMPT_PATH}") from exc
        return self._prompt_cache

    def load_jobs(self) -> List[Job]:
        if not self.should_run():
            return []

        categories = self._fetch_categories()
        filtered_categories = filter_by_range(
            categories, tuple(self.config.range.categories)
        )

        jobs: List[Job] = []
        for category in filtered_categories:
            subcategories = self._fetch_subcategories(category["id"])
            filtered_subcats = filter_by_range(
                subcategories, tuple(self.config.range.subcategories)
            )
            pending_subcats = [
                sub
                for sub in filtered_subcats
                if self._status(sub.get("ready")) != "READY"
            ]
            if not pending_subcats:
                logger.info(
                    "[BUSINESS] Skip subcategory localization | category={} reason=all_ready",
                    category["id"],
                )
                continue
            payload = {
                "category_id": category["id"],
                "category_name": category["name"],
                "subcategories": [
                    {
                        "id": sub["id"],
                        "name": sub["name"],
                    }
                    for sub in pending_subcats
                ],
            }
            jobs.append(
                make_job(
                    self.NAME,
                    payload,
                    key_fields=["category_id"],
                )
            )

        return jobs

    def process(self, job: Job) -> None:
        batch = SubcategoryBatch(
            category_id=job.payload["category_id"],
            category_name=job.payload["category_name"],
            subcategories=job.payload["subcategories"],
        )

        logger.info(
            "[BUSINESS] Start subcategory localization",
            category_id=batch.category_id,
            total=len(batch.subcategories),
        )

        messages = self._build_messages(batch)

        try:
            response = self.llm_client.chat(
                messages,
                model=self.llm_model,
                response_schema=self._get_response_schema(),
            )
        except (httpx.HTTPError, RetryableStepError) as exc:
            raise RetryableStepError(f"LLM localization failed: {exc}") from exc
        except Exception as exc:
            raise RetryableStepError(f"Unexpected LLM error: {exc}") from exc

        content = self._extract_content(response)
        expected_ids = {int(item["id"]) for item in batch.subcategories}
        localization_map = self._parse_localization(content, expected_ids)
        missing = expected_ids.difference(localization_map.keys())
        if missing:
            raise RetryableStepError(
                f"LLM response missing subcategories: {sorted(missing)}"
            )
        self._persist_localizations(localization_map)

        logger.info(
            "[BUSINESS] Subcategories localized",
            category_id=batch.category_id,
            total=len(localization_map),
        )

    def _fetch_categories(self) -> List[Dict[str, Any]]:
        try:
            response = (
                self.supabase.table("categories")
                .select("id, name, position")
                .order("position")
                .execute()
            )
        except Exception as exc:
            raise FatalStepError(f"Failed to load categories: {exc}") from exc
        return getattr(response, "data", []) or []

    def _fetch_subcategories(self, category_id: int) -> List[Dict[str, Any]]:
        try:
            response = (
                self.supabase.table("subcategories")
                .select("id, name, position, ready")
                .eq("category_id", category_id)
                .order("position")
                .execute()
            )
        except Exception as exc:
            raise FatalStepError(
                f"Failed to load subcategories for category {category_id}: {exc}"
            ) from exc
        return getattr(response, "data", []) or []

    def _build_messages(self, batch: SubcategoryBatch) -> List[Dict[str, str]]:
        payload = {
            "category": {
                "id": batch.category_id,
                "name": batch.category_name,
            },
            "subcategories": batch.subcategories,
            "languages": self.config.languages,
        }
        return [
            {"role": "system", "content": self._prompt()},
            {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
        ]

    def _extract_content(self, response: Dict[str, Any]) -> str:
        choices = response.get("choices") or []
        if not choices:
            raise ValueError("Empty response choices")
        message_block = choices[0].get("message") or {}
        content = message_block.get("content")
        if not content:
            raise ValueError("Empty response content")
        return content

    def _parse_localization(
        self, content: str, expected_ids: set[int]
    ) -> Dict[int, Dict[str, Any]]:
        try:
            parsed = json.loads(content)
        except json.JSONDecodeError as exc:
            raise RetryableStepError(f"Invalid JSON from LLM: {exc}") from exc

        items = parsed.get("items")
        if not isinstance(items, list):
            raise RetryableStepError("LLM response missing 'items' array")

        localization_map: Dict[int, Dict[str, Any]] = {}
        for item in items:
            sub_id = item.get("subcategory_id")
            female = item.get("female")
            male = item.get("male")
            if sub_id is None or female is None or male is None:
                raise RetryableStepError("Incomplete localization entry in LLM response")
            sub_id_int = int(sub_id)
            if sub_id_int not in expected_ids:
                raise RetryableStepError(
                    f"LLM response contains unexpected subcategory id: {sub_id_int}"
                )
            localization_map[sub_id_int] = {"female": female, "male": male}
        return localization_map

    def _persist_localizations(self, localization_map: Dict[int, Dict[str, Any]]) -> None:
        updates = [
            {"id": sub_id, "localization": localization, "ready": "CHECK"}
            for sub_id, localization in localization_map.items()
        ]
        try:
            for update in updates:
                (
                    self.supabase.table("subcategories")
                    .update(
                        {
                            "localization": update["localization"],
                            "ready": update["ready"],
                        }
                    )
                    .eq("id", update["id"])
                    .execute()
                )
        except Exception as exc:
            raise RetryableStepError(f"Failed to update subcategories: {exc}") from exc

    def _get_response_schema(self) -> Type[BaseModel]:
        if self._response_schema is None:
            title_model = create_model(
                "SubcategoryTitle",
                **{lang: (str, ...) for lang in self.config.languages},
            )
            sex_model = create_model(
                "SubcategorySexLocalization",
                title=(title_model, ...),
            )
            item_model = create_model(
                "SubcategoryLocalizationItem",
                subcategory_id=(int, ...),
                female=(sex_model, ...),
                male=(sex_model, ...),
            )
            self._response_schema = create_model(
                "SubcategoryLocalizationResponse",
                items=(List[item_model], ...),  # type: ignore[name-defined]
            )
        return self._response_schema

    @staticmethod
    def _status(value: Optional[str]) -> str:
        return (value or "").strip().upper()


__all__ = ["Step2LocalizeSubcategories"]
