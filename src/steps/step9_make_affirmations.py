from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
import re
import time
from typing import Any, Callable, Dict, List, Optional, Type, Tuple, Set
from threading import Lock, Thread

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
class MakeAffirmationJob:
    category: Dict[str, Any]
    subcategory: Dict[str, Any]
    coach: Dict[str, Any]
    gender: str
    language: str


class MakeAffirmationItemModel(BaseModel):
    affirmation: str = Field(...)
    scene: str = Field(...)

    model_config = ConfigDict(extra="forbid")


class MakeAffirmationResponseModel(BaseModel):
    female: Optional[List[MakeAffirmationItemModel]] = None
    male: Optional[List[MakeAffirmationItemModel]] = None

    model_config = ConfigDict(extra="forbid")


class Step9MakeAffirmations(BaseStep):
    NAME = "make_affirmations"
    PROMPT_PATH = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "agents"
        / "make_affirmations.md"
    )

    def __init__(self, config: Config) -> None:
        super().__init__(config)
        if not config.languages:
            raise FatalStepError("Config must include at least one language")
        if not config.versions:
            raise FatalStepError("config.versions must list target coaches")
        if config.affirmations_per_subcategory <= 0:
            raise FatalStepError(
                "affirmations_per_subcategory must be greater than 0")

        self.supabase = get_supabase_client()
        self.llm_client = LLMClient()
        self.languages = [lang.strip().upper()
                          for lang in config.languages if lang.strip()]
        self._prompt_cache: Optional[str] = None
        self._response_model: Optional[Type[BaseModel]] = None
        self.target_count = config.affirmations_per_subcategory
        self.model_id = (
            config.ids.get("make_affirmations_model")
            or config.ids.get("affirmations_model")
            or "gpt-5"
        )
        self.llm_attempt_timeout = 300
        self.total_combos_per_pair = 2 * len(self.languages)
        self._pair_lock = Lock()
        self._pair_stats: Dict[Tuple[Any, Any], Dict[str, Any]] = {}
        self._summary_logged = False
        self._slot_lock = Lock()
        self._active_slots: Set[Tuple[Any, Any, int]] = set()

    # --------------------------------------------------------------------- jobs
    def load_jobs(self) -> List[Job]:
        if not self.should_run():
            return []

        coaches = self._fetch_coaches()
        if not coaches:
            logger.warning(
                "[BUSINESS] Skip make_affirmations | reason=no_coaches",
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
                    ready_cache: Dict[Tuple[str, str], int] = {}
                    if not self.config.regenerate_affirmations:
                        existing = self._fetch_affirmations(subcategory["id"], coach["id"])
                        ready_cache = self._count_ready_by_combo(existing)
                    for gender in ("female", "male"):
                        for language in self.languages:
                            if (
                                not self.config.regenerate_affirmations
                                and ready_cache.get((gender, language), 0) >= self.target_count
                            ):
                                logger.info(
                                    "[BUSINESS] Skip make_affirmations job | reason=ready target met sub{} {} {}/{} ready={}",
                                    subcategory["id"],
                                    coach.get("coach"),
                                    "F" if gender == "female" else "M",
                                    language,
                                    ready_cache.get((gender, language), 0),
                                )
                                continue
                            payload = {
                                "category": {
                                    "id": category["id"],
                                    "name": category.get("name"),
                                },
                                "subcategory": {
                                    "id": subcategory["id"],
                                    "name": subcategory.get("name"),
                                    "localization": subcategory.get("localization"),
                                },
                                "coach": {
                                    "id": coach["id"],
                                    "coach": coach.get("coach"),
                                    "prompt_w": coach.get("coach_prompt_w"),
                                    "prompt_m": coach.get("coach_prompt_m"),
                                },
                                "gender": gender,
                                "language": language,
                                "subcategory_id": subcategory["id"],
                                "coach_id": coach["id"],
                            }
                            jobs.append(
                                make_job(
                                    self.NAME,
                                    payload,
                                    key_fields=[
                                        "subcategory_id",
                                        "coach_id",
                                        "gender",
                                        "language",
                                    ],
                                )
                            )
        return jobs

    # ------------------------------------------------------------------ process
    def process(self, job: Job) -> None:
        payload = MakeAffirmationJob(
            category=job.payload["category"],
            subcategory=job.payload["subcategory"],
            coach=job.payload["coach"],
            gender=job.payload["gender"],
            language=job.payload["language"],
        )

        subcategory_id = payload.subcategory["id"]
        coach_id = payload.coach["id"]
        coach_name = payload.coach.get("coach")

        target = self.target_count
        localization = self._parse_localization(
            payload.subcategory.get("localization"))
        coach_prompt = self._resolve_coach_prompt(
            payload.coach, payload.gender)
        if not coach_prompt:
            logger.warning(
                "make_affirmations skipped | reason=missing_coach_prompt",
                subcategory=subcategory_id,
                coach=coach_name,
                gender=payload.gender,
                language=payload.language,
            )
            self._record_pair_result(
                subcategory_id, coach_id, coach_name, payload.gender, payload.language, False
            )
            return

        localized_title = self._resolve_localized_title(
            localization, payload.gender, payload.language
        )
        if not localized_title:
            logger.warning(
                "make_affirmations skipped | reason=missing_localization",
                subcategory=subcategory_id,
                coach=coach_name,
                gender=payload.gender,
                language=payload.language,
            )
            self._record_pair_result(
                subcategory_id, coach_id, coach_name, payload.gender, payload.language, False
            )
            return

        gender_code = "F" if payload.gender == "female" else "M"
        job_attempt = int(job.payload.get("_attempt", 1) or 1)
        max_job_attempts = max(1, self.config.retry.attempts)
        job_attempt = min(job_attempt, max_job_attempts)
        start_label = "START" if job_attempt == 1 else f"START #{job_attempt}"
        logger.info(
            "[BUSINESS] make_affirmations {} | sub{} {} {}/{} target={}",
            start_label,
            subcategory_id,
            coach_name,
            gender_code,
            payload.language,
            target,
        )

        system_prompt = self._compose_system_prompt(
            coach_prompt, payload.gender)

        try:
            items = self._request_affirmations(
                subcategory_id=subcategory_id,
                coach_name=coach_name,
                gender_code=gender_code,
                gender=payload.gender,
                language=payload.language,
                target=target,
                system_prompt=system_prompt,
                localized_title=localized_title,
                context_hint=self._build_context_hint(payload.subcategory.get("name")),
                job_attempt=job_attempt,
                max_attempts=max_job_attempts,
            )
        except RetryableStepError as exc:
            logger.warning(
                "make_affirmations skipped | reason=llm_request_failed",
                subcategory=subcategory_id,
                coach=coach_name,
                gender=payload.gender,
                language=payload.language,
                error=str(exc),
            )
            if job_attempt >= max_job_attempts:
                self._record_pair_result(
                    subcategory_id,
                    coach_id,
                    coach_name,
                    payload.gender,
                    payload.language,
                    False,
                )
            raise

        saved_total = self._upsert_records(
            subcategory_id=subcategory_id,
            coach_id=coach_id,
            gender=payload.gender,
            language=payload.language,
            items=items,
        )

        if saved_total == 0:
            logger.warning(
                "make_affirmations skipped | reason=no_changes",
                subcategory=subcategory_id,
                coach=coach_name,
                gender=payload.gender,
                language=payload.language,
            )
            self._record_pair_result(
                subcategory_id, coach_id, coach_name, payload.gender, payload.language, False
            )
            return

        logger.info(
            "[BUSINESS] make_affirmations DONE | sub{} {} {}/{} saved={}",
            subcategory_id,
            coach_name,
            gender_code,
            payload.language,
            saved_total,
        )
        self._record_pair_result(
            subcategory_id, coach_id, coach_name, payload.gender, payload.language, True
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
                .select("id, name, position, localization")
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
        desired = [item.strip()
                   for item in self.config.versions if item.strip()]
        if not desired:
            return []
        try:
            response = (
                self.supabase.table("coaches")
                .select("id, coach, coach_prompt_w, coach_prompt_m")
                .in_("coach", desired)
                .execute()
            )
        except Exception as exc:
            raise FatalStepError(f"Failed to load coaches: {exc}") from exc

        coaches = getattr(response, "data", []) or []
        missing = sorted(set(desired).difference(
            {row.get("coach") for row in coaches}))
        if missing:
            logger.warning("Some configured coaches not found",
                           missing=missing)
        return coaches

    def _fetch_affirmations(
        self, subcategory_id: Any, coach_id: Any
    ) -> List[Dict[str, Any]]:
        try:
            response = (
                self.supabase.table("affirmations_new")
                .select("id, position, script")
                .eq("subcategory_id", subcategory_id)
                .eq("coach_id", coach_id)
                .order("position")
                .execute()
            )
        except Exception as exc:
            raise RetryableStepError(
                f"Failed to load affirmations_new for subcategory {subcategory_id}: {exc}"
            ) from exc
        return getattr(response, "data", []) or []

    def _count_ready_by_combo(
        self, records: List[Dict[str, Any]]
    ) -> Dict[Tuple[str, str], int]:
        counts: Dict[Tuple[str, str], int] = {}
        for record in records or []:
            script_map = self._parse_script(record.get("script"))
            for gender_key in ("female", "male"):
                lang_block = script_map.get(gender_key)
                if not isinstance(lang_block, dict):
                    continue
                for lang_code, entry in lang_block.items():
                    if not isinstance(entry, dict):
                        continue
                    title = str(entry.get("title", "")).strip()
                    scene = str(entry.get("script", "")).strip()
                    if not (title and scene):
                        continue
                    key = (gender_key, str(lang_code).strip().upper())
                    counts[key] = counts.get(key, 0) + 1
        return counts

    def _resolve_coach_prompt(self, coach: Dict[str, Any], gender: str) -> Optional[str]:
        key = "prompt_w" if gender == "female" else "prompt_m"
        prompt = coach.get(key)
        if not prompt:
            return None
        return str(prompt)

    def _parse_localization(self, raw: Any) -> Dict[str, Any]:
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except json.JSONDecodeError:
                return {}
        return raw if isinstance(raw, dict) else {}

    def _resolve_localized_title(
        self,
        localization: Dict[str, Any],
        gender: str,
        language: str,
    ) -> Optional[str]:
        gender_block = localization.get(gender)
        if not isinstance(gender_block, dict):
            return None
        title_block = gender_block.get("title")
        if not isinstance(title_block, dict):
            return None
        value = title_block.get(language)
        return str(value).strip() if value else None

    @staticmethod
    def _build_context_hint(subcategory_name: Optional[str]) -> str:
        base = str(subcategory_name or "").strip()
        return f"{base} affirmation" if base else "affirmation"

    def _base_prompt(self) -> str:
        if self._prompt_cache is None:
            try:
                self._prompt_cache = self.PROMPT_PATH.read_text(
                    encoding="utf-8")
            except FileNotFoundError as exc:
                raise FatalStepError(
                    f"Prompt file not found: {self.PROMPT_PATH}"
                ) from exc
        return self._prompt_cache

    def _compose_system_prompt(self, coach_prompt: str, gender: str) -> str:
        base = self._base_prompt()
        prompt = (coach_prompt or "").strip()
        system_text = f"{prompt}\n\n{base}" if prompt else base
        if gender == "male":
            system_text = system_text.replace(
                "female", "male").replace("женского", "мужского")
        return system_text

    def _response_schema(self) -> Type[BaseModel]:
        if self._response_model is None:
            self._response_model = MakeAffirmationResponseModel
        return self._response_model

    def _request_affirmations(
        self,
        *,
        subcategory_id: Any,
        coach_name: str,
        gender_code: str,
        gender: str,
        language: str,
        target: int,
        system_prompt: str,
        localized_title: str,
        context_hint: str,
        job_attempt: int,
        max_attempts: int,
    ) -> List[Dict[str, str]]:
        payload = {
            "language": language,
            "target": target,
            "affirmation": localized_title,
            "context": context_hint,
        }
        system_message = {"role": "system", "content": system_prompt}
        user_content = "[INPUT]\n" + json.dumps(payload, ensure_ascii=False)
        user_message = {"role": "user", "content": user_content}

        messages = [system_message, user_message]
        result: Dict[str, Any] = {}

        def _run_llm() -> None:
            try:
                result["response"] = self.llm_client.chat(
                    messages,
                    model=self.model_id,
                    response_schema=self._response_schema(),
                )
            except Exception as exc:  # pragma: no cover - propagates via result
                result["error"] = exc

        thread = Thread(target=_run_llm, daemon=True)
        thread.start()
        thread.join(self.llm_attempt_timeout)
        if thread.is_alive():
            logger.warning(
                "LLM request timed out | attempt={}/{} sub{} {} {}/{} timeout={}s",
                job_attempt,
                max(1, max_attempts),
                subcategory_id,
                coach_name,
                gender_code,
                language,
                self.llm_attempt_timeout,
            )
            raise RetryableStepError(
                f"LLM request timed out after {self.llm_attempt_timeout}s"
            )

        if "error" in result:
            raise RetryableStepError(
                f"Unexpected LLM error: {result['error']}")

        response = result.get("response")
        if response is None:
            raise RetryableStepError("LLM request returned empty response")

        content = self._extract_content(response)
        data = self._parse_json(content)
        items = data.get(gender)
        if not isinstance(items, list):
            raise RetryableStepError(
                f"LLM response missing list for gender {gender}"
            )
        if len(items) < target:
            raise RetryableStepError(
                f"LLM items mismatch (expected {target}, got {len(items)})"
            )
        if len(items) > target:
            logger.warning(
                "LLM returned extra items; trimming",
                expected=target,
                received=len(items),
                gender=gender,
                language=language,
            )
            items = items[:target]
        cleaned: List[Dict[str, str]] = []
        for item in items:
            affirmation = str((item or {}).get("affirmation", "")).strip()
            scene = str((item or {}).get("scene", "")).strip()
            if not affirmation or not scene:
                raise RetryableStepError(
                    "LLM response missing affirmation/scene content"
                )
            cleaned.append(
                {
                    "title": self._sanitize_title(affirmation),
                    "script": self._sanitize_script(scene),
                }
            )
        return cleaned

    def _extract_content(self, response: Dict[str, Any]) -> str:
        choices = response.get("choices") or []
        if not choices:
            raise RetryableStepError("Empty response choices from LLM")
        message = choices[0].get("message") or {}
        content = message.get("content")
        if not content:
            raise RetryableStepError("Empty response content from LLM")
        return content

    def _parse_json(self, content: str) -> Dict[str, Any]:
        try:
            data = json.loads(content)
        except json.JSONDecodeError as exc:
            raise RetryableStepError(f"Invalid JSON from LLM: {exc}") from exc
        if not isinstance(data, dict):
            raise RetryableStepError("LLM response root must be an object")
        return data

    def _upsert_records(
        self,
        *,
        subcategory_id: Any,
        coach_id: Any,
        gender: str,
        language: str,
        items: List[Dict[str, str]],
    ) -> int:
        saved = 0
        for position, item in enumerate(items, start=1):
            slot_key = (subcategory_id, coach_id, position)
            self._acquire_slot(slot_key)
            try:
                record = self._fetch_record(subcategory_id, coach_id, position)
                script_map = self._parse_script(
                    record.get("script") if record else None)
                gender_block = script_map.setdefault(gender, {})
                gender_block[language] = {
                    "title": item["title"],
                    "script": item["script"],
                }
                script_payload = self._format_json(script_map)
                ready_count = self._count_ready_entries(script_map)

                if record:
                    self._update_record(
                        record["id"],
                        script_payload,
                        ready_count,
                        gender,
                        language,
                    )
                else:
                    insert_payload = {
                        "subcategory_id": subcategory_id,
                        "coach_id": coach_id,
                        "position": position,
                        "script": script_payload,
                        "ready_counter": ready_count,
                        "music": None,
                    }
                    self._insert_record(
                        insert_payload, ready_count, gender, language)
            finally:
                self._release_slot(slot_key)
            saved += 1
        return saved

    def _record_pair_result(
        self,
        subcategory_id: Any,
        coach_id: Any,
        coach_name: str,
        gender: str,
        language: str,
        success: bool,
    ) -> None:
        gender_code = "F" if gender == "female" else "M"
        with self._pair_lock:
            entry = self._pair_stats.setdefault(
                (subcategory_id, coach_id),
                {
                    "coach": coach_name,
                    "remaining": self.total_combos_per_pair,
                    "failures": [],
                },
            )
            entry["remaining"] = max(0, entry["remaining"] - 1)
            if not success:
                entry["failures"].append(f"{gender_code}/{language}")
            if entry["remaining"] == 0:
                failures = entry["failures"]
                if failures:
                    if not self._summary_logged:
                        logger.error("ЗАВЕРШЕНО С ОШИБКАМИ")
                        self._summary_logged = True
                    logger.error(
                        "! Sub{} {} - {}/{} ({})",
                        subcategory_id,
                        entry["coach"],
                        len(failures),
                        self.total_combos_per_pair,
                        ", ".join(failures),
                    )
                self._pair_stats.pop((subcategory_id, coach_id), None)

    @staticmethod
    def _strip_newlines(text: str) -> str:
        normalized = text.replace("\r\n", "\n")
        normalized = normalized.replace("\n\n", " ")
        normalized = normalized.replace("\n", " ")
        return normalized

    def _sanitize_script(self, text: str) -> str:
        normalized = self._strip_newlines(text)
        normalized = normalized.replace('"', "'")
        normalized = normalized.replace("'", "'")
        normalized = re.sub(r"[“”«»„‟‹›‚‘’]", "'", normalized)
        normalized = normalized.replace("\\'", "'")
        normalized = normalized.replace('\\"', "'")
        return normalized.strip()

    @staticmethod
    def _sanitize_title(text: str) -> str:
        return Step9MakeAffirmations._strip_newlines(text).strip()

    def _fetch_record(
        self, subcategory_id: Any, coach_id: Any, position: int
    ) -> Optional[Dict[str, Any]]:
        def op() -> Optional[Dict[str, Any]]:
            response = (
                self.supabase.table("affirmations_new")
                .select("id, script, music")
                .eq("subcategory_id", subcategory_id)
                .eq("coach_id", coach_id)
                .eq("position", position)
                .limit(1)
                .execute()
            )
            data = getattr(response, "data", []) or []
            return data[0] if data else None

        return self._with_db_retry(
            f"Failed to fetch affirmations_new record (pos {position})",
            op,
        )

    def _insert_record(
        self,
        payload: Dict[str, Any],
        ready_count: int,
        gender: str,
        language: str,
    ) -> None:
        def op() -> None:
            response = self.supabase.table(
                "affirmations_new").insert(payload).execute()
            inserted = getattr(response, "data", []) or [{}]
            row = inserted[0]
            logger.info(
                "[DATA] Supabase INSERT | table=affirmations_new id={} sub{} coach={} position={} {}/{} ready={}",
                row.get("id"),
                row.get("subcategory_id"),
                row.get("coach_id"),
                row.get("position"),
                gender,
                language,
                ready_count,
            )

        desc = (
            f"Failed to insert affirmations_new (sub{payload['subcategory_id']}"
            f" coach={payload['coach_id']} position={payload['position']})"
        )
        self._with_db_retry(desc, op)

    def _update_record(
        self,
        record_id: Any,
        script_payload: Any,
        ready_count: int,
        gender: str,
        language: str,
    ) -> None:
        def op() -> None:
            response = (
                self.supabase.table("affirmations_new")
                .update({"script": script_payload, "ready_counter": ready_count})
                .eq("id", record_id)
                .execute()
            )
            updated = getattr(response, "data", []) or [{}]
            logger.info(
                "[DATA] Supabase UPDATE | table=affirmations_new id={} {}/{} ready={}",
                updated[0].get("id", record_id),
                gender,
                language,
                ready_count,
            )

        self._with_db_retry(
            f"Failed to update affirmations_new id={record_id}", op
        )

    @staticmethod
    def _count_ready_entries(script_map: Dict[str, Any]) -> int:
        total = 0
        for gender_key in ("female", "male"):
            gender_block = script_map.get(gender_key)
            if not isinstance(gender_block, dict):
                continue
            for lang_data in gender_block.values():
                if not isinstance(lang_data, dict):
                    continue
                title = str(lang_data.get("title", "")).strip()
                script = str(lang_data.get("script", "")).strip()
                if title and script:
                    total += 1
        return total

    def _acquire_slot(self, key: Tuple[Any, Any, int]) -> None:
        while True:
            with self._slot_lock:
                if key not in self._active_slots:
                    self._active_slots.add(key)
                    return
            logger.info(
                "[DATA] Waiting for slot | sub{} coach={} position={}",
                key[0],
                key[1],
                key[2],
            )
            time.sleep(3)

    def _release_slot(self, key: Tuple[Any, Any, int]) -> None:
        with self._slot_lock:
            self._active_slots.discard(key)

    def _with_db_retry(self, op_desc: str, func: Callable[[], Any]) -> Any:
        attempts = max(1, self.config.db_retry.attempts)
        for attempt in range(1, attempts + 1):
            try:
                return func()
            except Exception as exc:
                if attempt == attempts:
                    raise RetryableStepError(f"{op_desc}: {exc}") from exc
                delay = self._db_retry_delay(attempt)
                logger.warning(
                    "{} failed | attempt={}/{} error={}",
                    op_desc,
                    attempt,
                    attempts,
                    exc,
                )
                if delay > 0:
                    time.sleep(delay)

    def _db_retry_delay(self, attempt: int) -> float:
        delays = self.config.db_retry.delays_sec or [0.0]
        idx = min(max(attempt - 1, 0), len(delays) - 1)
        return max(0.0, delays[idx])

    def _parse_script(self, raw: Any) -> Dict[str, Any]:
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except json.JSONDecodeError:
                raw = None
        if not isinstance(raw, dict):
            raw = {"female": {}, "male": {}}
        raw.setdefault("female", {})
        raw.setdefault("male", {})
        return raw

    @staticmethod
    def _format_json(data: Any) -> Any:
        return json.loads(json.dumps(data, ensure_ascii=False, sort_keys=True))


__all__ = ["Step9MakeAffirmations"]
