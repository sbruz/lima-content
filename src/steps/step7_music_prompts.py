from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
import re
from typing import Any, Dict, List, Optional, Type

import httpx
from pydantic import BaseModel, Field, ValidationError, create_model

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
class MusicJob:
    category: Dict[str, Any]
    subcategory: Dict[str, Any]
    coach: Dict[str, Any]


class Step7MusicPrompts(BaseStep):
    NAME = "music_prompts"
    PROMPT_PATH = (
        Path(__file__).resolve().parents[2] / "docs" / "agents" / "affirmations_music.md"
    )

    def __init__(self, config: Config) -> None:
        super().__init__(config)
        if not config.versions:
            raise FatalStepError("config.versions must contain at least one coach")
        if not config.languages:
            raise FatalStepError("Config must include at least one language for music prompts")

        self.supabase = get_supabase_client()
        self.llm_client = LLMClient()
        self.languages = [lang.strip().upper() for lang in config.languages if lang.strip()]
        self._prompt_cache: Optional[str] = None
        self._response_model: Optional[Type[BaseModel]] = None
        self.music_tail_sec = max(0.0, float(config.music_prompt_tail_sec or 0.0))

    # --------------------------------------------------------------------- jobs
    def load_jobs(self) -> List[Job]:
        if not self.should_run():
            return []

        coaches = self._fetch_coaches()
        if not coaches:
            logger.warning(
                "[BUSINESS] Skip music_prompts | reason=no_coaches",
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
                            "position": category.get("position"),
                        },
                        "subcategory": {
                            "id": subcategory["id"],
                            "name": subcategory.get("name"),
                            "position": subcategory.get("position"),
                        },
                        "coach": {
                            "id": coach["id"],
                            "coach": coach.get("coach"),
                            "prompt": coach.get("prompt"),
                            "description": coach.get("coach_description"),
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
        payload = MusicJob(
            category=job.payload["category"],
            subcategory=job.payload["subcategory"],
            coach=job.payload["coach"],
        )
        subcategory_id = payload.subcategory["id"]
        coach_id = payload.coach["id"]

        logger.info(
            "[BUSINESS] Start music_prompts | subcategory={} coach={}",
            subcategory_id,
            payload.coach.get("coach"),
        )

        records = self._fetch_affirmations(subcategory_id, coach_id)
        if not records:
            logger.warning(
                "No affirmations to generate music prompts",
                subcategory=subcategory_id,
                coach=payload.coach.get("coach"),
            )
            return

        record_contexts: List[Dict[str, Any]] = []
        total_tasks = 0
        for record in records:
            record_id = record.get("id")
            script = self._parse_script_json(record.get("script"))
            if not script:
                logger.warning(
                    "Skipping record without script",
                    record_id=record_id,
                )
                continue
            combos = self._count_script_combinations(script)
            if combos == 0:
                logger.warning(
                    "No script entries for required languages",
                    record_id=record_id,
                )
                continue
            duration_map = self._parse_duration_json(record.get("duration"))
            existing_music = self._parse_music_json(record.get("music"))
            record_contexts.append(
                {
                    "record": record,
                    "script": script,
                    "duration_map": duration_map,
                    "music_payload": self._ensure_music_payload(existing_music),
                }
            )
            total_tasks += combos

        if total_tasks == 0:
            logger.warning(
                "No eligible affirmations for music prompts",
                subcategory=subcategory_id,
                coach=payload.coach.get("coach"),
            )
            return

        logger.info(
            "[BUSINESS] Start music_prompts | subcategory={} coach={} combos={}",
            subcategory_id,
            payload.coach.get("coach"),
            total_tasks,
        )

        updates: List[Dict[str, Any]] = []
        completed = 0
        for ctx in record_contexts:
            record = ctx["record"]
            music_payload = ctx["music_payload"]
            script = ctx["script"]
            duration_map = ctx["duration_map"]
            record_id = record.get("id")

            for language in self.languages:
                language_iso = language.lower()
                for gender in ("female", "male"):
                    script_text = self._extract_script_text(script, gender, language)
                    if not script_text:
                        continue

                    duration_sec = self._resolve_duration_seconds(
                        job.payload,
                        record,
                        gender,
                        language_iso,
                        duration_map,
                    )

                    request_payload = self._build_request_payload(
                        payload,
                        script_text,
                        duration_sec,
                    )

                    job_code = self._build_job_code(
                        job.payload, record, language, gender
                    )
                    logger.info(
                        "[BUSINESS] Music request | job={} aff_id={} duration={:.2f}s chars={}",
                        job_code,
                        record_id,
                        duration_sec,
                        len(script_text),
                    )

                    prompt_text = self._request_prompt(request_payload)
                    clean_prompt = self._sanitize_prompt(prompt_text)
                    music_payload.setdefault(gender, {}).setdefault(language, {})[
                        "prompt"
                    ] = clean_prompt

                    completed += 1
                    logger.info(
                        "[BUSINESS] Music prompt generated | job={} aff_id={} prompt_chars={} done={}/{}",
                        job_code,
                        record_id,
                        len(prompt_text),
                        completed,
                        total_tasks,
                    )
                    logger.info(
                        "[PROG] Music progress | done={}/{}",
                        completed,
                        total_tasks,
                    )

            updates.append(
                {
                    "id": record_id,
                    "music": self._format_json(music_payload),
                }
            )

        if updates:
            self._persist_updates(updates)

        logger.info(
            "[BUSINESS] Music prompts generated | subcategory={} coach={} count={}",
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
                .select("id, coach, prompt, coach_description")
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
                self.supabase.table("affirmations_new")
                .select("id, position, script, duration, music")
                .eq("subcategory_id", subcategory_id)
                .eq("coach_id", coach_id)
                .order("position")
                .execute()
            )
        except Exception as exc:
            raise RetryableStepError(
                f"Failed to load affirmations for subcategory {subcategory_id}: {exc}"
            ) from exc
        records = getattr(response, "data", []) or []
        ready_records = [record for record in records if record.get("script")]
        missing = len(records) - len(ready_records)
        if missing > 0:
            logger.warning(
                "Skipping positions without scripts",
                subcategory=subcategory_id,
                coach=coach_id,
                missing=missing,
            )
        filtered = filter_by_range(
            ready_records,
            tuple(self.config.range.positions),
        )
        return filtered

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
            self._response_model = create_model(
                "MusicPromptResponse",
                prompt=(str, Field(...)),
            )
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


    def _persist_updates(self, updates: List[Dict[str, Any]]) -> None:
        try:
            for update in updates:
                (
                    self.supabase.table("affirmations_new")
                    .update({"music": update["music"]})
                    .eq("id", update["id"])
                    .execute()
                )
        except Exception as exc:
            raise RetryableStepError(f"Failed to persist music prompts: {exc}") from exc

    def _format_json(self, data: Any) -> Any:
        return json.loads(json.dumps(data, ensure_ascii=False, sort_keys=True))

    @staticmethod
    def _to_float(value: Any) -> Optional[float]:
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _parse_script_json(self, raw: Any) -> Dict[str, Any]:
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise RetryableStepError(f"Invalid script JSON: {exc}") from exc
        return raw if isinstance(raw, dict) else {}

    def _parse_duration_json(self, raw: Any) -> Dict[str, Any]:
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except json.JSONDecodeError:
                return {}
        return raw if isinstance(raw, dict) else {}

    def _parse_music_json(self, raw: Any) -> Dict[str, Any]:
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except json.JSONDecodeError:
                return {}
        return raw if isinstance(raw, dict) else {}

    def _ensure_music_payload(self, existing: Dict[str, Any]) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"female": {}, "male": {}}
        if not isinstance(existing, dict):
            return payload
        for gender in ("female", "male"):
            entries: Dict[str, Any] = {}
            source = existing.get(gender) if isinstance(existing.get(gender), dict) else {}
            if isinstance(source, dict):
                for language, value in source.items():
                    if isinstance(value, dict):
                        entries[language] = dict(value)
                    elif isinstance(value, str):
                        entries[language] = {"prompt": value}
            payload[gender] = entries
        return payload

    @staticmethod
    def _strip_newlines(text: str) -> str:
        normalized = text.replace("\r\n", "\n")
        normalized = normalized.replace("\n\n", " ")
        normalized = normalized.replace("\n", " ")
        return normalized

    @staticmethod
    def _sanitize_prompt(text: str) -> str:
        normalized = Step7MusicPrompts._strip_newlines(text)
        normalized = normalized.replace('"', "'")
        normalized = normalized.replace("'", "'")
        normalized = re.sub(r"[“”«»„‟‹›‚‘’]", "'", normalized)
        normalized = normalized.replace("\\'", "'")
        normalized = normalized.replace('\\"', "'")
        return normalized.strip()

    def _extract_script_text(
        self, script: Dict[str, Any], gender: str, language: str
    ) -> Optional[str]:
        gender_block = script.get(gender) or {}
        lang_entry = gender_block.get(language)
        if isinstance(lang_entry, dict):
            text = lang_entry.get("script") or lang_entry.get("prompt")
        elif isinstance(lang_entry, str):
            text = lang_entry
        else:
            text = None
        if text:
            return str(text).strip()
        return None

    def _resolve_duration_seconds(
        self,
        job_payload: Dict[str, Any],
        record: Dict[str, Any],
        gender: str,
        language_iso: str,
        duration_map: Dict[str, Any],
    ) -> float:
        cat_pos = int(job_payload["category"].get("position") or 0)
        sub_pos = int(job_payload["subcategory"].get("position") or 0)
        coach_id = job_payload["coach"].get("id")
        record_pos = int(record.get("position") or 0)
        gender_code = "w" if gender == "female" else "m"
        filename = (
            f"{cat_pos}_{sub_pos}_{coach_id}_{record_pos}_{gender_code}_{language_iso}.mp3"
        )
        value = self._to_float(duration_map.get(filename))
        if value is not None:
            return round(value, 3)
        logger.warning(
            "Missing duration for music prompt",
            filename=filename,
            record_id=record.get("id"),
        )
        return 120.0

    def _build_request_payload(
        self,
        job: MusicJob,
        script_text: str,
        duration_sec: float,
    ) -> Dict[str, Any]:
        target_duration = max(0.0, duration_sec + self.music_tail_sec)
        coach_desc = (
            job.coach.get("description")
            or job.coach.get("prompt")
            or job.coach.get("coach")
        )
        return {
            "category": job.category.get("name"),
            "subcategory": job.subcategory.get("name"),
            "coach_description": coach_desc,
            "affirmation_script": script_text,
            "affirmation_duration": target_duration,
        }

    def _request_prompt(self, payload: Dict[str, Any]) -> str:
        messages = [
            {"role": "system", "content": self._prompt_text()},
            {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
        ]
        try:
            response = self.llm_client.chat(
                messages,
                model=self.config.ids.get("affirmations_music_model", "gpt-5"),
                response_schema=self._response_schema(),
            )
        except (httpx.HTTPError, RetryableStepError) as exc:
            logger.error("Music prompt LLM request failed", payload=payload, error=str(exc))
            raise RetryableStepError(f"LLM request failed: {exc}") from exc
        except Exception as exc:  # pragma: no cover - safety net
            logger.error("Unexpected LLM error", payload=payload, error=str(exc))
            raise RetryableStepError(f"Unexpected LLM error: {exc}") from exc

        content = self._extract_content(response)
        return self._parse_prompt(content)

    def _parse_prompt(self, content: str) -> str:
        try:
            schema = self._response_schema()
            model: BaseModel = schema.model_validate_json(content)
        except (ValidationError, ValueError) as exc:
            raise RetryableStepError(f"Invalid LLM response: {exc}") from exc
        prompt = getattr(model, "prompt", "").strip()
        if not prompt:
            raise RetryableStepError("LLM response missing prompt")
        return prompt

    def _count_script_combinations(self, script: Dict[str, Any]) -> int:
        combos = 0
        for language in self.languages:
            for gender in ("female", "male"):
                if self._extract_script_text(script, gender, language):
                    combos += 1
        return combos

    def _build_job_code(
        self,
        job_payload: Dict[str, Any],
        record: Dict[str, Any],
        language: str,
        gender: str,
    ) -> str:
        category = job_payload.get("category") or {}
        subcategory = job_payload.get("subcategory") or {}
        coach_id = job_payload.get("coach", {}).get("id")
        cat_pos = int(category.get("position") or 0)
        sub_pos = int(subcategory.get("position") or 0)
        record_pos = int(record.get("position") or 0)
        gender_code = gender[0].upper()
        language_code = language.upper()
        return f"C{cat_pos}-S{sub_pos}-N{coach_id}-P{record_pos}-{language_code}-{gender_code}"


__all__ = ["Step7MusicPrompts"]
