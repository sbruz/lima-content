from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Type

import httpx
from pydantic import BaseModel

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
class ScriptJob:
    category: Dict[str, Any]
    subcategory: Dict[str, Any]
    coach: Dict[str, Any]


class AffirmationLanguageModel(BaseModel):
    title: str
    script: str


class AffirmationPairModel(BaseModel):
    female: AffirmationLanguageModel
    male: AffirmationLanguageModel


class AffirmationResponseModel(BaseModel):
    affirmation: AffirmationPairModel


class Step5ScriptAffirmations(BaseStep):
    NAME = "script_affirmations"
    PROMPT_TRANSLATE_PATH = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "agents"
        / "affirmations_script_translate.md"
    )
    PROMPT_PAUSES_PATH = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "agents"
        / "affirmations_script_pauses.md"
    )

    def __init__(self, config: Config) -> None:
        super().__init__(config)
        if not config.languages:
            raise FatalStepError("Config must include at least one language")
        if not config.versions:
            raise FatalStepError("config.versions must contain at least one coach")

        self.supabase = get_supabase_client()
        self.llm_client = LLMClient()
        self.languages = [item.strip().upper() for item in config.languages if item.strip()]
        self.add_pauses = config.script_affirmations_add_pauses
        self._prompt_translate_cache: Optional[str] = None
        self._prompt_pauses_cache: Optional[str] = None
        self._response_model: Optional[Type[BaseModel]] = None

    # --------------------------------------------------------------------- jobs
    def load_jobs(self) -> List[Job]:
        if not self.should_run():
            return []

        coaches = self._fetch_coaches()
        if not coaches:
            logger.warning(
                "[BUSINESS] Skip script_affirmations | reason=no_coaches",
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
                    records = self._fetch_affirmations(subcategory["id"], coach["id"])
                    if not records:
                        logger.warning(
                            "No affirmations to script",
                            subcategory=subcategory["id"],
                            coach=coach["coach"],
                        )
                        continue
                    for record in records:
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
                            "record": record,
                            "record_id": record.get("id"),
                            "record_position": record.get("position"),
                        }
                        jobs.append(
                            make_job(
                                self.NAME,
                                payload,
                                key_fields=[
                                    "subcategory_id",
                                    "coach_id",
                                    "record_id",
                                ],
                            )
                        )
        return jobs

    # ------------------------------------------------------------------ process
    def process(self, job: Job) -> None:
        payload = ScriptJob(
            category=job.payload["category"],
            subcategory=job.payload["subcategory"],
            coach=job.payload["coach"],
        )
        subcategory_id = payload.subcategory["id"]
        coach_id = payload.coach["id"]

        logger.info(
            "[BUSINESS] Start script_affirmations | subcategory={} coach={} position={}",
            subcategory_id,
            payload.coach.get("coach"),
            job.payload.get("record_position"),
        )

        record = job.payload["record"]
        records = [record]
        affirmation_payload = self._prepare_affirmation_payload(record)
        localization: Dict[int, Dict[str, Dict[str, str]]] = {
            int(record.get("position")): {"female": {}, "male": {}}
        }

        translation_plan = {lang.upper(): lang.upper() != "EN" for lang in self.languages}
        total_subtasks = 0
        for lang in self.languages:
            if translation_plan.get(lang.upper(), False):
                total_subtasks += 1
            if self.add_pauses:
                total_subtasks += 1
        if total_subtasks == 0:
            total_subtasks = 1

        completed_subtasks = 0
        started_at = time.perf_counter()
        position = job.payload.get("record_position")
        position_tag = f"P{position}" if position is not None else "P?"
        job_code = f"{payload.subcategory['id']}:{payload.coach.get('coach')}:{position_tag}"

        for language in self.languages:
            lang_code = language.upper()

            if translation_plan.get(lang_code, False):
                translation_result = self._run_subtask(
                    payload,
                    affirmation_payload,
                    lang_code,
                    self._translation_prompt(),
                )
                completed_subtasks += 1
                self._log_progress(
                    job_code,
                    lang_code,
                    "TRNS",
                    completed_subtasks,
                    total_subtasks,
                    started_at,
                )
            else:
                translation_result = affirmation_payload
                self._log_progress(
                    job_code,
                    lang_code,
                    "TRNS",
                    completed_subtasks,
                    total_subtasks,
                    started_at,
                    skipped=True,
                )

            sanitized_input = self._sanitize_affirmation_scripts(translation_result)

            if self.add_pauses:
                pause_result = self._run_subtask(
                    payload,
                    sanitized_input,
                    lang_code,
                    self._pauses_prompt(),
                )
                completed_subtasks += 1
                self._log_progress(
                    job_code,
                    lang_code,
                    "PAUS",
                    completed_subtasks,
                    total_subtasks,
                    started_at,
                )
            else:
                pause_result = sanitized_input

            for position, entry in localization.items():
                entry["female"][lang_code] = pause_result["female"]
                entry["male"][lang_code] = pause_result["male"]

        updates = self._build_updates(records, localization)
        self._persist_updates(updates)

        logger.info(
            "[BUSINESS] Scripts generated | subcategory={} coach={} count={}",
            subcategory_id,
            payload.coach.get("coach"),
            len(updates),
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

    def _fetch_affirmations(
        self, subcategory_id: Any, coach_id: Any
    ) -> List[Dict[str, Any]]:
        try:
            response = (
                self.supabase.table("affirmations")
                .select("id, position, affirmation")
                .eq("subcategory_id", subcategory_id)
                .eq("coach_id", coach_id)
                .order("position")
                .execute()
            )
        except Exception as exc:
            raise RetryableStepError(
                f"Failed to load affirmations for subcategory {subcategory_id}: {exc}"
            ) from exc
        return getattr(response, "data", []) or []

    def _translation_prompt(self) -> str:
        if self._prompt_translate_cache is None:
            try:
                self._prompt_translate_cache = self.PROMPT_TRANSLATE_PATH.read_text(
                    encoding="utf-8"
                )
            except FileNotFoundError as exc:
                raise FatalStepError(
                    f"Prompt file not found: {self.PROMPT_TRANSLATE_PATH}"
                ) from exc
        return self._prompt_translate_cache

    def _pauses_prompt(self) -> str:
        if self._prompt_pauses_cache is None:
            try:
                self._prompt_pauses_cache = self.PROMPT_PAUSES_PATH.read_text(
                    encoding="utf-8"
                )
            except FileNotFoundError as exc:
                raise FatalStepError(
                    f"Prompt file not found: {self.PROMPT_PAUSES_PATH}"
                ) from exc
        return self._prompt_pauses_cache

    def _prepare_affirmation_payload(
        self, record: Dict[str, Any]
    ) -> Dict[str, Dict[str, str]]:
        affirmation = record.get("affirmation")
        if isinstance(affirmation, str):
            try:
                affirmation = json.loads(affirmation)
            except json.JSONDecodeError as exc:
                raise RetryableStepError(
                    f"Invalid affirmation JSON for record {record.get('id')}: {exc}"
                ) from exc
        if not isinstance(affirmation, dict):
            raise RetryableStepError(
                f"Affirmation payload missing for record {record.get('id')}"
            )
        female = affirmation.get("female")
        male = affirmation.get("male")
        if not isinstance(female, dict) or not isinstance(male, dict):
            raise RetryableStepError(
                f"Affirmation payload missing female/male for record {record.get('id')}"
            )
        return {
            "female": {
                "title": str(female.get("affirmation", "")).strip(),
                "script": str(female.get("scene", "")).strip(),
            },
            "male": {
                "title": str(male.get("affirmation", "")).strip(),
                "script": str(male.get("scene", "")).strip(),
            },
        }

    def _build_messages(
        self,
        job: ScriptJob,
        affirmation: Dict[str, Dict[str, str]],
        language: str,
        prompt_text: str,
    ) -> List[Dict[str, str]]:
        payload = {
            "category": job.category.get("name"),
            "subcategory": job.subcategory.get("name"),
            "coach": job.coach.get("coach"),
            "coach_prompt": job.coach.get("prompt"),
            "target_language": language,
            "affirmation": affirmation,
        }
        return [
            {"role": "system", "content": prompt_text},
            {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
        ]

    def _run_subtask(
        self,
        job: ScriptJob,
        affirmation: Dict[str, Dict[str, str]],
        language: str,
        prompt_text: str,
        *,
        sanitize_script: bool = False,
    ) -> Dict[str, Dict[str, str]]:
        affirmation_payload = (
            self._sanitize_affirmation_scripts(affirmation)
            if sanitize_script
            else affirmation
        )
        messages = self._build_messages(job, affirmation_payload, language, prompt_text)
        try:
            response = self.llm_client.chat(
                messages,
                model=self.config.ids.get("affirmations_script_model", "gpt-5"),
                response_schema=self._response_schema(),
            )
        except (httpx.HTTPError, RetryableStepError) as exc:
            raise RetryableStepError(f"LLM request failed: {exc}") from exc
        except Exception as exc:  # pragma: no cover - safety net
            raise RetryableStepError(f"Unexpected LLM error: {exc}") from exc

        content = self._extract_content(response)
        return self._parse_single_language(content)

    def _response_schema(self) -> Type[BaseModel]:
        if self._response_model is None:
            self._response_model = AffirmationResponseModel
        return self._response_model

    def _extract_content(self, response: Dict[str, Any]) -> str:
        choices = response.get("choices") or []
        if not choices:
            raise RetryableStepError("Empty response choices from LLM")
        message_block = choices[0].get("message") or {}
        content = message_block.get("content")
        if not content:
            raise RetryableStepError("Empty response content from LLM")
        return content

    def _parse_single_language(
        self, content: str
    ) -> Dict[str, Dict[str, str]]:
        try:
            data = json.loads(content)
        except json.JSONDecodeError as exc:
            logger.error("Failed to parse LLM response", error=str(exc), raw_response=content)
            raise RetryableStepError(f"Invalid JSON from LLM: {exc}") from exc

        affirmation = data.get("affirmation")
        if not isinstance(affirmation, dict):
            raise RetryableStepError("LLM response missing 'affirmation' object")

        female_block = self._extract_gender_entry(affirmation.get("female"))
        male_block = self._extract_gender_entry(affirmation.get("male"))
        return {"female": female_block, "male": male_block}

    def _extract_gender_entry(self, block: Any) -> Dict[str, str]:
        if not isinstance(block, dict):
            raise RetryableStepError("LLM response missing gender block")
        title = str(block.get("title", "")).strip()
        script = str(block.get("script", "")).strip()
        if not title or not script:
            raise RetryableStepError(
                "LLM response missing title/script for gender block"
            )
        return {"title": title, "script": script}

    def _build_updates(
        self,
        records: List[Dict[str, Any]],
        parsed: Dict[int, Dict[str, Dict[str, Dict[str, str]]]],
    ) -> List[Dict[str, Any]]:
        index_by_position = {int(record.get("position")): record for record in records}
        updates: List[Dict[str, Any]] = []

        for position, payload in parsed.items():
            record = index_by_position.get(position)
            if not record:
                logger.warning(
                    "Skipping script for unknown position",
                    position=position,
                )
                continue

            female = payload.get("female", {})
            male = payload.get("male", {})
            missing_langs = [
                lang
                for lang in self.languages
                if lang not in female or lang not in male
            ]
            if missing_langs:
                raise RetryableStepError(
                    f"Missing languages in LLM response for position {position}: {missing_langs}"
                )

            clean_female = self._sanitize_output_block(female)
            clean_male = self._sanitize_output_block(male)

            updates.append(
                {
                    "id": record.get("id"),
                    "script": self._format_json(
                        {
                            "female": clean_female,
                            "male": clean_male,
                        }
                    ),
                }
            )
        return updates

    def _persist_updates(self, updates: List[Dict[str, Any]]) -> None:
        try:
            for update in updates:
                (
                    self.supabase.table("affirmations")
                    .update({"script": update["script"]})
                    .eq("id", update["id"])
                    .execute()
                )
        except Exception as exc:
            raise RetryableStepError(f"Failed to persist scripts: {exc}") from exc

    @staticmethod
    def _format_json(data: Any) -> Any:
        return json.loads(json.dumps(data, ensure_ascii=False, sort_keys=True))

    def _sanitize_affirmation_scripts(
        self, affirmation: Dict[str, Dict[str, str]]
    ) -> Dict[str, Dict[str, str]]:
        cleaned: Dict[str, Dict[str, str]] = {}
        for gender in ("female", "male"):
            block = affirmation.get(gender) or {}
            cleaned[gender] = {
                "title": str(block.get("title", "")),
                "script": self._strip_newlines(str(block.get("script", ""))),
            }
        return cleaned

    @staticmethod
    def _strip_newlines(text: str) -> str:
        normalized = text.replace("\r\n", "\n")
        normalized = normalized.replace("\n\n", " ")
        normalized = normalized.replace("\n", " ")
        return normalized

    def _sanitize_output_block(
        self, block: Dict[str, Dict[str, str]]
    ) -> Dict[str, Dict[str, str]]:
        cleaned: Dict[str, Dict[str, str]] = {}
        for lang, entry in block.items():
            cleaned[lang] = {
                "title": str(entry.get("title", "")).strip(),
                "script": self._normalize_script_for_storage(
                    str(entry.get("script", "")).strip()
                ),
            }
        return cleaned

    def _normalize_script_for_storage(self, text: str) -> str:
        normalized = text
        normalized = normalized.replace('"', "'")
        normalized = normalized.replace("'", "'")
        normalized = re.sub(r"[“”«»„‟‹›‚‘’]", "'", normalized)
        normalized = normalized.replace("\\'", "'")
        normalized = normalized.replace('\\"', "'")
        return normalized

    def _log_progress(
        self,
        job_code: str,
        language: str,
        stage: str,
        completed: int,
        total: int,
        started_at: float,
        *,
        skipped: bool = False,
    ) -> None:
        ratio = max(0.0, min(1.0, completed / total if total else 1.0))
        bar_len = 20
        filled = int(ratio * bar_len)
        bar = "#" * filled + "-" * (bar_len - filled)
        elapsed = time.perf_counter() - started_at
        avg = elapsed / completed if completed else 0
        remaining = max(total - completed, 0)
        eta = self._format_eta(int(avg * remaining))
        percent = ratio * 100
        stage_code = f"{stage}{'*' if skipped else ''}"
        logger.info(
            "[PROG] {}:{}:{} [{}] {:5.1f}% ETA={}",
            job_code,
            language,
            stage_code,
            bar,
            percent,
            eta,
        )

    @staticmethod
    def _format_eta(seconds: int) -> str:
        minutes, sec = divmod(max(seconds, 0), 60)
        return f"{minutes:02d}:{sec:02d}"


__all__ = ["Step5ScriptAffirmations"]
