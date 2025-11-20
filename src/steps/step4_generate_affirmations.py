from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Type

import httpx
from pydantic import BaseModel, Field, ConfigDict

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
class AffirmationJob:
    category: Dict[str, Any]
    subcategory: Dict[str, Any]
    coach: Dict[str, Any]


class AffirmationItemModel(BaseModel):
    affirmation: str = Field(...)
    scene: str = Field(...)

    model_config = ConfigDict(extra="forbid")


class AffirmationPayloadModel(BaseModel):
    female: List[AffirmationItemModel] = Field(...)
    male: List[AffirmationItemModel] = Field(...)

    model_config = ConfigDict(extra="forbid")


class Step4GenerateAffirmations(BaseStep):
    NAME = "generate_affirmations"
    PROMPT_PATH = (
        Path(__file__).resolve().parents[2] / "docs" / "agents" / "affirmations_base.md"
    )

    def __init__(self, config: Config) -> None:
        super().__init__(config)
        if config.affirmations_per_subcategory <= 0:
            raise FatalStepError("affirmations_per_subcategory must be greater than 0")
        if not config.versions:
            raise FatalStepError("config.versions must contain at least one coach name")

        self.supabase = get_supabase_client()
        self.llm_client = LLMClient()
        self._prompt_cache: Optional[str] = None
        self._response_model: Optional[Type[BaseModel]] = None
        self.affirmations_per_subcategory = config.affirmations_per_subcategory

    # --------------------------------------------------------------------- jobs
    def load_jobs(self) -> List[Job]:
        if not self.should_run():
            return []

        coaches = self._fetch_coaches()
        if not coaches:
            logger.warning(
                "[BUSINESS] Skip generate_affirmations | reason=no_coaches",
                versions=self.config.versions,
            )
            return []

        categories = self._fetch_categories()
        filtered_categories = filter_by_range(
            categories, tuple(self.config.range.categories)
        )

        jobs: List[Job] = []
        for category in filtered_categories:
            subcategories = self._fetch_subcategories(category["id"])
            filtered_subcategories = filter_by_range(
                subcategories, tuple(self.config.range.subcategories)
            )
            for subcategory in filtered_subcategories:
                for coach in coaches:
                    payload = {
                        "category": {
                            "id": category["id"],
                            "name": category.get("name"),
                        },
                        "subcategory": {
                            "id": subcategory["id"],
                            "name": subcategory.get("name"),
                        },
                        "coach": {
                            "id": coach["id"],
                            "coach": coach.get("coach"),
                            "prompt": coach.get("prompt"),
                        },
                        "subcategory_id": subcategory["id"],
                        "coach_id": coach["id"],
                    }
                    jobs.append(
                        make_job(
                            self.NAME,
                            payload,
                            key_fields=["subcategory_id", "coach_id"],
                        )
                    )
        return jobs

    # ------------------------------------------------------------------ process
    def process(self, job: Job) -> None:
        payload = AffirmationJob(
            category=job.payload["category"],
            subcategory=job.payload["subcategory"],
            coach=job.payload["coach"],
        )

        sub_id = payload.subcategory["id"]
        coach_id = payload.coach["id"]

        logger.info(
            "[BUSINESS] Start generate_affirmations | subcategory={} coach={}",
            sub_id,
            payload.coach.get("coach"),
        )

        messages = self._build_messages(payload)
        try:
            response = self.llm_client.chat(
                messages,
                model=self.config.ids.get("affirmations_model", "gpt-5"),
                response_schema=self._response_schema(),
            )
        except (httpx.HTTPError, RetryableStepError) as exc:
            raise RetryableStepError(f"LLM request failed: {exc}") from exc
        except Exception as exc:  # pragma: no cover - safety net
            raise RetryableStepError(f"Unexpected LLM error: {exc}") from exc

        content = self._extract_content(response)
        parsed = self._parse_affirmations(content)
        female_items = parsed.get("female", [])
        male_items = parsed.get("male", [])

        expected = self.affirmations_per_subcategory
        if len(female_items) != expected or len(male_items) != expected:
            logger.warning(
                "Affirmation count mismatch",
                subcategory=sub_id,
                coach=payload.coach.get("coach"),
                expected=expected,
                female=len(female_items),
                male=len(male_items),
            )

        records = self._build_records(sub_id, coach_id, female_items, male_items)
        self._persist_records(sub_id, coach_id, records)

        logger.info(
            "[BUSINESS] Affirmations generated | subcategory={} coach={} count={}",
            sub_id,
            payload.coach.get("coach"),
            len(records),
        )

    # ----------------------------------------------------------------- helpers
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

    def _fetch_subcategories(self, category_id: Any) -> List[Dict[str, Any]]:
        try:
            response = (
                self.supabase.table("subcategories")
                .select("id, name, position")
                .eq("category_id", category_id)
                .order("position")
                .execute()
            )
        except Exception as exc:
            raise FatalStepError(
                f"Failed to load subcategories for category {category_id}: {exc}"
            ) from exc
        return getattr(response, "data", []) or []

    def _fetch_coaches(self) -> List[Dict[str, Any]]:
        desired = [item.strip() for item in self.config.versions if item.strip()]
        if not desired:
            return []

        try:
            response = (
                self.supabase.table("coaches")
                .select("id, coach, prompt")
                .in_("coach", desired)
                .execute()
            )
        except Exception as exc:
            raise FatalStepError(f"Failed to load coaches: {exc}") from exc

        coaches = getattr(response, "data", []) or []
        missing = sorted(set(desired).difference({row.get("coach") for row in coaches}))
        if missing:
            logger.warning("Some configured coaches not found", missing=missing)
        return coaches

    def _prompt_text(self) -> str:
        if self._prompt_cache is None:
            try:
                self._prompt_cache = self.PROMPT_PATH.read_text(encoding="utf-8")
            except FileNotFoundError as exc:
                raise FatalStepError(
                    f"Prompt file not found: {self.PROMPT_PATH}"
                ) from exc
        return self._prompt_cache

    def _response_schema(self) -> Type[BaseModel]:
        if self._response_model is None:
            self._response_model = AffirmationPayloadModel
        return self._response_model

    def _build_messages(self, job: AffirmationJob) -> List[Dict[str, str]]:
        payload = {
            "category": job.category.get("name"),
            "subcategory": job.subcategory.get("name"),
            "coach": job.coach.get("coach"),
            "coach_prompt": job.coach.get("prompt"),
            "target": self.affirmations_per_subcategory,
        }
        messages: List[Dict[str, str]] = [
            {"role": "system", "content": self._prompt_text()},
            {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
        ]
        return messages

    def _extract_content(self, response: Dict[str, Any]) -> str:
        choices = response.get("choices") or []
        if not choices:
            raise RetryableStepError("Empty response choices from LLM")
        message_block = choices[0].get("message") or {}
        content = message_block.get("content")
        if not content:
            raise RetryableStepError("Empty response content from LLM")
        return content

    def _parse_affirmations(self, content: str) -> Dict[str, List[Dict[str, str]]]:
        try:
            parsed = json.loads(content)
        except json.JSONDecodeError as exc:
            raise RetryableStepError(f"Invalid JSON from LLM: {exc}") from exc

        female = parsed.get("female")
        male = parsed.get("male")
        if not isinstance(female, list) or not isinstance(male, list):
            raise RetryableStepError("LLM response missing female/male arrays")
        return {"female": female, "male": male}

    def _build_records(
        self,
        subcategory_id: Any,
        coach_id: Any,
        female_items: List[Dict[str, Any]],
        male_items: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        records: List[Dict[str, Any]] = []
        count = min(len(female_items), len(male_items))
        for index in range(count):
            female = female_items[index]
            male = male_items[index]
            record = {
                "subcategory_id": subcategory_id,
                "coach_id": coach_id,
                "position": index + 1,
                "affirmation": self._format_json(
                    {
                        "female": {
                            "affirmation": str(female.get("affirmation", "")).strip(),
                            "scene": str(female.get("scene", "")).strip(),
                        },
                        "male": {
                            "affirmation": str(male.get("affirmation", "")).strip(),
                            "scene": str(male.get("scene", "")).strip(),
                        },
                    }
                ),
                "script": None,
                "music": None,
            }
            records.append(record)
        return records

    def _persist_records(
        self, subcategory_id: Any, coach_id: Any, records: List[Dict[str, Any]]
    ) -> None:
        try:
            (
                self.supabase.table("affirmations")
                .delete()
                .eq("subcategory_id", subcategory_id)
                .eq("coach_id", coach_id)
                .execute()
            )
            if records:
                self.supabase.table("affirmations").insert(records).execute()
        except Exception as exc:
            raise RetryableStepError(
                f"Failed to persist affirmations for subcategory {subcategory_id}, coach {coach_id}: {exc}"
            ) from exc

    @staticmethod
    def _format_json(data: Any) -> Any:
        """Return JSON-compatible structure with deterministic key order."""
        return json.loads(json.dumps(data, ensure_ascii=False, sort_keys=True))


__all__ = ["Step4GenerateAffirmations"]
