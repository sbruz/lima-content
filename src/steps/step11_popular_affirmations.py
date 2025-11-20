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


class PopularAffirmationResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")
    line: str = Field(..., min_length=1)


class Step11PopularAffirmations(BaseStep):
    NAME = "popular_affirmations"
    GENDERS = ("female", "male")

    def __init__(self, config: Config) -> None:
        super().__init__(config)
        if not config.languages:
            raise FatalStepError("Config must include at least one language")
        self.supabase = get_supabase_client()
        self.llm_client = LLMClient()
        self.languages = [lang.strip().upper() for lang in config.languages if lang.strip()]
        self.prompt_path = Path("docs/agents/popular_affirmation.md")
        if not self.prompt_path.exists():
            raise FatalStepError(f"Prompt file not found: {self.prompt_path}")
        self.model_id = self.config.ids.get("popular_aff_model", "gpt-5")

    # --------------------------------------------------------------------- jobs
    def load_jobs(self) -> List[Job]:
        if not self.should_run():
            return []

        categories = filter_by_range(self._fetch_categories(), tuple(self.config.range.categories))
        coaches = self._fetch_coaches()
        if not coaches:
            logger.warning("[BUSINESS] Skip popular_affirmations | reason=no_coaches", versions=self.config.versions)
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
        subcategory_localization = self._parse_json(payload["subcategory"].get("localization"))
        script_map = self._parse_json(record.get("script"))
        if not script_map:
            logger.warning(
                "[BUSINESS] Popular aff skipped | reason=empty_script record={}",
                record_id,
            )
            return

        popular_map = self._parse_json(record.get("popular_aff"))
        updated = False

        for gender in self.GENDERS:
            gender_block = script_map.get(gender)
            if not isinstance(gender_block, dict):
                continue

            gender_pop = popular_map.get(gender)
            if not isinstance(gender_pop, dict):
                gender_pop = {}
            popular_map[gender] = gender_pop

            for language in self.languages:
                lang_entry = gender_block.get(language)
                if not isinstance(lang_entry, dict):
                    continue
                script_text = (lang_entry.get("script") or "").strip()
                title_text = self._extract_localized_title(subcategory_localization, gender, language)
                if not title_text:
                    title_text = (lang_entry.get("title") or "").strip()
                if not script_text:
                    continue

                self._log_progress(payload, gender, language)
                line = self._request_popular_line(title_text, script_text)
                gender_pop[language] = line
                updated = True

        if not updated:
            return
        self._persist_popular(record_id, popular_map)

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
                .select("id, name, position, localization")
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
            response = self.supabase.table("coaches").select("id, coach").in_("coach", desired).execute()
        except Exception as exc:
            raise FatalStepError(f"Failed to load coaches: {exc}") from exc
        return getattr(response, "data", []) or []

    def _fetch_affirmations(self, subcategory_id: Any, coach_id: Any) -> List[Dict[str, Any]]:
        try:
            response = (
                self.supabase.table("affirmations_new")
                .select("id, position, script, popular_aff")
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

    def _request_popular_line(self, title: str, script: str) -> str:
        prompt = self.prompt_path.read_text(encoding="utf-8")
        payload = {"title": title, "script": script}
        messages = [
            {"role": "system", "content": prompt},
            {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
        ]
        response = self.llm_client.chat(
            messages,
            model=self.model_id,
            response_schema=PopularAffirmationResponse,
        )
        try:
            parsed = self._parse_schema_response(response, PopularAffirmationResponse)
            data = parsed.model_dump() if isinstance(parsed, BaseModel) else parsed
            line = str(data.get("line") or "").strip()
            if not line:
                raise ValueError("Empty line returned")
            return line
        except Exception as exc:
            raise RetryableStepError(f"Invalid popular affirmation response: {exc}") from exc

    def _log_progress(self, payload: Dict[str, Any], gender: str, language: str) -> None:
        coach_id = payload["coach"].get("id")
        cat_pos = payload["category"].get("position")
        sub_pos = payload["subcategory"].get("position")
        record_pos = payload["record"].get("position")
        short_label = (
            f"C{cat_pos or '?'}-S{sub_pos or '?'}-N{coach_id or '?'}-P{record_pos or '?'}-"
            f"{(gender[:1].upper() if gender else '?')}-{language or '?'}"
        )
        logger.info("[BUSINESS] Popular aff | {}", short_label)

    def _persist_popular(self, record_id: Any, data: Dict[str, Any]) -> None:
        payload = self._format_json(data)
        try:
            (
                self.supabase.table("affirmations_new")
                .update({"popular_aff": payload})
                .eq("id", record_id)
                .execute()
            )
        except Exception as exc:
            raise RetryableStepError(f"Failed to update popular_aff for {record_id}: {exc}") from exc

    def _format_json(self, data: Any) -> Any:
        return json.loads(json.dumps(data, ensure_ascii=False, sort_keys=True))

    def _extract_localized_title(self, localization: Dict[str, Any], gender: str, language: str) -> Optional[str]:
        block = localization.get(gender) if isinstance(localization, dict) else None
        if not isinstance(block, dict):
            return None
        title_block = block.get("title")
        if isinstance(title_block, dict):
            value = title_block.get(language)
        elif isinstance(title_block, str):
            value = title_block
        else:
            value = None
        return str(value).strip() if value else None

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


__all__ = ["Step11PopularAffirmations"]
