from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field

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


class CoachAffirmationForTimeOfDay(BaseModel):
    model_config = ConfigDict(extra="forbid")
    line: str = Field(..., min_length=1)


class Step12CoachAffirmationForTimeOfDay(BaseStep):
    NAME = "coach_affirmation_for_time_of_day"
    GENDERS = ("female", "male")
    TIMES_OF_DAY = ("morning", "afternoon", "late evening")
    MAX_CHARS = 70
    CHAR_LIMIT_ATTEMPTS = 5

    def __init__(self, config: Config) -> None:
        super().__init__(config)
        if not config.languages:
            raise FatalStepError("Config must include at least one language")
        self.supabase = get_supabase_client()
        self.llm_client = LLMClient()
        self.languages = [lang.strip().upper() for lang in config.languages if lang.strip()]
        self.prompt_path = Path("docs/agents/coach_aff_style.md")
        if not self.prompt_path.exists():
            raise FatalStepError(f"Prompt file not found: {self.prompt_path}")
        self.model_id = self.config.ids.get("coach_aff_time_model", "gpt-5.1")

    # --------------------------------------------------------------------- jobs
    def load_jobs(self) -> List[Job]:
        if not self.should_run():
            return []

        categories = filter_by_range(self._fetch_categories(), tuple(self.config.range.categories))
        coaches = self._fetch_coaches()
        if not coaches:
            logger.warning(
                "[BUSINESS] Skip coach_affirmation_for_time_of_day | reason=no_coaches",
                versions=self.config.versions,
            )
            return []

        jobs: List[Job] = []
        for category in categories:
            subcategories = filter_by_range(
                self._fetch_subcategories(category["id"]),
                tuple(self.config.range.subcategories),
            )
            for subcategory in subcategories:
                for coach in coaches:
                    records = filter_by_range(
                        self._fetch_affirmations(subcategory["id"], coach["id"]),
                        tuple(self.config.range.positions),
                    )
                    for record in records:
                        if not record.get("popular_aff"):
                            continue
                        payload = {
                            "category": category,
                            "subcategory": subcategory,
                            "coach": coach,
                            "record": record,
                            "subcategory_id": subcategory["id"],
                            "coach_id": coach["id"],
                            "record_id": record.get("id"),
                        }
                        jobs.append(
                            make_job(
                                self.NAME,
                                payload,
                                key_fields=["subcategory_id", "coach_id", "record_id"],
                            )
                        )
        return jobs

    # ------------------------------------------------------------------ process
    def process(self, job: Job) -> None:
        payload = job.payload
        record = payload["record"]
        record_id = record["id"]
        coach = payload["coach"]
        coach_style = (coach.get("coach_aff_style") or "").strip()

        popular_map = self._parse_json(record.get("popular_aff"))
        if not popular_map:
            logger.warning(
                "[BUSINESS] Gentle aff skipped | reason=empty_popular record={}",
                record_id,
            )
            return

        banners_map = self._parse_json(record.get("aff_for_banners"))
        if not isinstance(banners_map, dict):
            banners_map = {}

        updated = False
        failures: List[str] = []
        missing_before: List[str] = []
        targets: List[tuple[str, str]] = []

        for gender in self.GENDERS:
            gender_block = popular_map.get(gender)
            if not isinstance(gender_block, dict):
                continue

            banner_gender = banners_map.get(gender)
            if not isinstance(banner_gender, dict):
                banner_gender = {}
            gender_had_content = bool(banner_gender)

            for language in self.languages:
                source_text = gender_block.get(language)
                if not isinstance(source_text, str) or not source_text.strip():
                    continue
                targets.append((gender, language))

                lang_block = banner_gender.get(language)
                if not isinstance(lang_block, dict):
                    lang_block = {}
                lang_had_content = bool(lang_block)

                missing_times = self._find_missing_times(banners_map, gender, language)
                if missing_times:
                    missing_before.extend(f"{gender}:{language}:{time}" for time in missing_times)
                else:
                    continue

                for time_of_day in missing_times:
                    self._log_progress(payload, gender, language, time_of_day)
                    line = self._generate_line(
                        technical_affirmation=source_text,
                        coach_adjustment=coach_style,
                        gender=gender,
                        language=language,
                        time_of_day=time_of_day,
                    )
                    if line:
                        lang_block[time_of_day] = line
                        updated = True
                    else:
                        failures.append(f"{record_id}:{gender}:{language}:{time_of_day}")

                if lang_block or lang_had_content:
                    banner_gender[language] = lang_block

            if banner_gender or gender_had_content:
                banners_map[gender] = banner_gender

        if failures:
            logger.error(
                "<red>[BUSINESS] Gentle aff failures | record={} items={}</red>",
                record_id,
                ", ".join(failures),
            )

        if updated:
            self._persist_banners(record_id, banners_map)

        missing_after = self._collect_missing(banners_map, targets)
        if missing_after:
            logger.error(
                "<red>[BUSINESS] Gentle aff missing after run | record={} missing_before={} missing_after={}</red>",
                record_id,
                ",".join(missing_before) if missing_before else "-",
                ",".join(missing_after),
            )
            raise RetryableStepError(
                f"Missing banner affirmations after generation: {record_id} missing={missing_after}"
            )
        if not updated:
            return

    # ----------------------------------------------------------------- helpers
    def _fetch_categories(self) -> List[Dict[str, Any]]:
        try:
            response = self.supabase.table("categories").select("id, name, position").order("position").execute()
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
            raise FatalStepError(f"Failed to load subcategories for category {category_id}: {exc}") from exc
        return getattr(response, "data", []) or []

    def _fetch_coaches(self) -> List[Dict[str, Any]]:
        desired = [item.strip() for item in self.config.versions if item.strip()]
        if not desired:
            return []
        try:
            response = (
                self.supabase.table("coaches")
                .select("id, coach, coach_aff_style")
                .in_("coach", desired)
                .execute()
            )
        except Exception as exc:
            raise FatalStepError(f"Failed to load coaches: {exc}") from exc
        return getattr(response, "data", []) or []

    def _fetch_affirmations(self, subcategory_id: Any, coach_id: Any) -> List[Dict[str, Any]]:
        try:
            response = (
                self.supabase.table("affirmations_new")
                .select("id, position, popular_aff, aff_for_banners")
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

    def _parse_json(self, raw: Any) -> Dict[str, Any]:
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except json.JSONDecodeError:
                return {}
        return raw if isinstance(raw, dict) else {}

    def _format_json(self, data: Any) -> Any:
        return json.loads(json.dumps(data, ensure_ascii=False, sort_keys=True))

    def _render_prompt(self, time_of_day: str, coach_adjustment: str) -> str:
        template = self.prompt_path.read_text(encoding="utf-8")
        return (
            template.replace("{time_of_day}", time_of_day)
            .replace("{coach_adjustment}", coach_adjustment)
        )

    def _find_missing_times(self, banners_map: Dict[str, Any], gender: str, language: str) -> List[str]:
        if not isinstance(banners_map, dict):
            return list(self.TIMES_OF_DAY)
        gender_block = banners_map.get(gender)
        if not isinstance(gender_block, dict):
            return list(self.TIMES_OF_DAY)
        lang_block = gender_block.get(language)
        if not isinstance(lang_block, dict):
            return list(self.TIMES_OF_DAY)
        missing = []
        for time_of_day in self.TIMES_OF_DAY:
            value = lang_block.get(time_of_day)
            if not isinstance(value, str) or not value.strip():
                missing.append(time_of_day)
        return missing

    def _collect_missing(self, banners_map: Dict[str, Any], targets: List[tuple[str, str]]) -> List[str]:
        missing: List[str] = []
        for gender, language in targets:
            for time_of_day in self._find_missing_times(banners_map, gender, language):
                missing.append(f"{gender}:{language}:{time_of_day}")
        return missing

    def _generate_line(
        self,
        *,
        technical_affirmation: str,
        coach_adjustment: str,
        gender: str,
        language: str,
        time_of_day: str,
    ) -> Optional[str]:
        prompt = self._render_prompt(time_of_day, coach_adjustment)
        payload = {
            "technical_affirmation": technical_affirmation,
            "gender": gender,
            "time_of_day": time_of_day,
        }
        messages = [
            {"role": "system", "content": prompt},
            {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
        ]

        last_length = 0
        for attempt in range(1, self.CHAR_LIMIT_ATTEMPTS + 1):
            response = self.llm_client.chat(
                messages,
                model=self.model_id,
                response_schema=CoachAffirmationForTimeOfDay,
            )
            try:
                parsed = self._parse_schema_response(response, CoachAffirmationForTimeOfDay)
                data = parsed.model_dump() if isinstance(parsed, BaseModel) else parsed
                line = str(data.get("line") or "").strip()
                if not line:
                    raise ValueError("Empty line returned")
            except Exception as exc:
                raise RetryableStepError(f"Invalid gentle affirmation response: {exc}") from exc

            last_length = len(line)
            if last_length <= self.MAX_CHARS:
                return line

            logger.warning(
                "[BUSINESS] Gentle aff retry | reason=char_limit chars={} attempt={} gender={} lang={} time={}",
                last_length,
                attempt,
                gender,
                language,
                time_of_day,
            )

        logger.error(
            "<red>[BUSINESS] Gentle aff failed | reason=char_limit chars={} gender={} lang={} time={}</red>",
            last_length,
            gender,
            language,
            time_of_day,
        )
        return None

    def _log_progress(self, payload: Dict[str, Any], gender: str, language: str, time_of_day: str) -> None:
        coach_id = payload["coach"].get("id")
        cat_pos = payload["category"].get("position")
        sub_pos = payload["subcategory"].get("position")
        record_pos = payload["record"].get("position")
        short_label = (
            f"C{cat_pos or '?'}-S{sub_pos or '?'}-N{coach_id or '?'}-P{record_pos or '?'}-"
            f"{(gender[:1].upper() if gender else '?')}-{language}-{time_of_day}"
        )
        logger.info("[BUSINESS] Gentle aff | {}", short_label)

    def _persist_banners(self, record_id: Any, data: Dict[str, Any]) -> None:
        payload = self._format_json(data)
        try:
            (
                self.supabase.table("affirmations_new")
                .update({"aff_for_banners": payload})
                .eq("id", record_id)
                .execute()
            )
        except Exception as exc:
            raise RetryableStepError(f"Failed to update aff_for_banners for {record_id}: {exc}") from exc

    def _extract_content(self, response: Dict[str, Any]) -> str:
        choices = response.get("choices") or []
        if not choices:
            raise RetryableStepError("Empty response choices from LLM")
        message = choices[0].get("message") or {}
        content = message.get("content")
        if not content:
            raise RetryableStepError("Empty response content from LLM")
        return content

    def _parse_schema_response(self, response: Any, schema: Any):
        if isinstance(response, dict) and "choices" not in response:
            return schema.model_validate(response)
        if isinstance(response, str):
            return schema.model_validate_json(response)
        if isinstance(response, dict) and "choices" in response:
            content = self._extract_content(response)
            return schema.model_validate_json(content)
        raise RetryableStepError("Unexpected LLM response format")


__all__ = ["Step12CoachAffirmationForTimeOfDay", "CoachAffirmationForTimeOfDay"]
