from __future__ import annotations

import base64
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from openai import OpenAI
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


class SuitabilityItem(BaseModel):
    model_config = ConfigDict(extra="forbid")
    suitable: bool = Field(...,
                           description="Whether the script fits this time of day")


class SuitabilityResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")
    morning: SuitabilityItem
    afternoon: SuitabilityItem
    night: SuitabilityItem


class ImagePromptResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")
    prompt: str
    ref: Optional[str] = None


class Step10DailyAffirmations(BaseStep):
    NAME = "daily_affirmations"
    PREVIEW_DIR = Path("export") / "daily_previews"
    TIMES = ("morning", "afternoon", "night")

    def __init__(self, config: Config) -> None:
        super().__init__(config)
        if not config.languages:
            raise FatalStepError("Config must include at least one language")
        self.supabase = get_supabase_client()
        self.llm_client = LLMClient()
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise FatalStepError("OPENAI_API_KEY is not configured")
        self.image_client = OpenAI(api_key=api_key)
        self.languages = [lang.strip().upper()
                          for lang in config.languages if lang.strip()]
        self._prompt_check = Path("docs/agents/check_daily_affirmation.md")
        self._prompt_image = Path("docs/agents/image_task.md")
        try:
            self.PREVIEW_DIR.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise FatalStepError(
                f"Cannot create preview directory {self.PREVIEW_DIR}: {exc}") from exc

    # --------------------------------------------------------------------- jobs
    def load_jobs(self) -> List[Job]:
        if not self.should_run():
            return []

        categories = self._fetch_categories()
        filtered_categories = filter_by_range(
            categories, tuple(self.config.range.categories))
        coaches = self._fetch_coaches()
        if not coaches:
            logger.warning(
                "[BUSINESS] Skip daily_affirmations | reason=no_coaches", versions=self.config.versions)
            return []

        jobs: List[Job] = []
        for category in filtered_categories:
            subcategories = self._fetch_subcategories(category["id"])
            filtered_subcategories = filter_by_range(
                subcategories, tuple(self.config.range.subcategories))
            for subcategory in filtered_subcategories:
                for coach in coaches:
                    records = self._fetch_affirmations(
                        subcategory["id"], coach["id"])
                    filtered_records = filter_by_range(
                        records, tuple(self.config.range.positions))
                    for record in filtered_records:
                        script_map = self._parse_script(record.get("script"))
                        if not script_map:
                            continue
                        for gender in ("female", "male"):
                            gender_block = script_map.get(gender)
                            if not isinstance(gender_block, dict):
                                continue
                            for language in self.languages:
                                lang_entry = gender_block.get(language)
                                if not isinstance(lang_entry, dict) or not lang_entry.get("script"):
                                    continue
                                payload = {
                                    "category": {
                                        "id": category["id"],
                                        "position": category.get("position"),
                                        "name": category.get("name"),
                                    },
                                    "subcategory": {
                                        "id": subcategory["id"],
                                        "position": subcategory.get("position"),
                                        "name": subcategory.get("name"),
                                    },
                                    "coach": {"id": coach["id"], "coach": coach.get("coach")},
                                    "record": {
                                        "id": record.get("id"),
                                        "position": record.get("position"),
                                        "script": record.get("script"),
                                        "allowed_for_daily": record.get("allowed_for_daily"),
                                    },
                                    "gender": gender,
                                    "language": language,
                                }
                                jobs.append(
                                    make_job(
                                        self.NAME,
                                        payload,
                                        key_fields=[
                                            "subcategory.id",
                                            "coach.id",
                                            "record.id",
                                            "gender",
                                            "language",
                                        ],
                                    )
                                )
        return jobs

    # ------------------------------------------------------------------ process
    def process(self, job: Job) -> None:
        payload = job.payload
        record = payload["record"]
        record_id = record["id"]
        gender = payload["gender"]
        language = payload["language"]
        script_map = self._parse_script(record.get("script"))
        script_text = self._extract_script(script_map, gender, language)
        if not script_text:
            logger.warning("daily_affirmations skipped | reason=no_script",
                           record_id=record_id, gender=gender, language=language)
            return

        allowed_map = self._parse_allowed(record.get("allowed_for_daily"))
        gender_block = allowed_map.setdefault(gender, {})
        lang_block = gender_block.setdefault(language, {})

        if not self._has_suitability(lang_block):
            suitability = self._request_suitability(script_text)
            lang_block.update(suitability)

        for time_of_day in self.TIMES:
            filename = self._build_preview_filename(
                payload, gender, language, time_of_day)
            if (self.PREVIEW_DIR / filename).exists():
                continue
            if not lang_block.get(time_of_day, {}).get("suitable", False):
                continue
            image_prompt = self._request_image_prompt(
                script_text, time_of_day, gender)
            lang_block[time_of_day] = {
                "suitable": True,
                "image_prompt": image_prompt,
            }
            image_bytes = self._generate_image(image_prompt.get("prompt"))
            (self.PREVIEW_DIR / filename).write_bytes(image_bytes)
            logger.info(
                "[BUSINESS] Daily preview saved | record={} gender={} lang={} time={} file={}",
                record_id,
                gender,
                language,
                time_of_day,
                filename,
            )

        self._persist_allowed(record_id, allowed_map)

    # ----------------------------------------------------------------- helpers
    def _fetch_categories(self) -> List[Dict[str, Any]]:
        try:
            response = self.supabase.table("categories").select(
                "id, name, position").order("position").execute()
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
                f"Failed to load subcategories for category {category_id}: {exc}") from exc
        return getattr(response, "data", []) or []

    def _fetch_coaches(self) -> List[Dict[str, Any]]:
        desired = [item.strip()
                   for item in self.config.versions if item.strip()]
        if not desired:
            return []
        try:
            response = self.supabase.table("coaches").select(
                "id, coach").in_("coach", desired).execute()
        except Exception as exc:
            raise FatalStepError(f"Failed to load coaches: {exc}") from exc
        return getattr(response, "data", []) or []

    def _fetch_affirmations(self, subcategory_id: Any, coach_id: Any) -> List[Dict[str, Any]]:
        try:
            response = (
                self.supabase.table("affirmations_new")
                .select("id, position, script, allowed_for_daily")
                .eq("subcategory_id", subcategory_id)
                .eq("coach_id", coach_id)
                .order("position")
                .execute()
            )
        except Exception as exc:
            raise RetryableStepError(
                f"Failed to load affirmations_new for subcategory {subcategory_id}: {exc}") from exc
        return getattr(response, "data", []) or []

    def _parse_script(self, raw: Any) -> Dict[str, Any]:
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except json.JSONDecodeError:
                return {}
        return raw if isinstance(raw, dict) else {}

    def _parse_allowed(self, raw: Any) -> Dict[str, Any]:
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except json.JSONDecodeError:
                return {}
        return raw if isinstance(raw, dict) else {}

    def _format_json(self, data: Any) -> Any:
        return json.loads(json.dumps(data, ensure_ascii=False, sort_keys=True))

    def _extract_script(self, script_map: Dict[str, Any], gender: str, language: str) -> Optional[str]:
        block = script_map.get(gender)
        if not isinstance(block, dict):
            return None
        lang_entry = block.get(language)
        if isinstance(lang_entry, dict):
            text = lang_entry.get("script") or lang_entry.get("prompt")
        elif isinstance(lang_entry, str):
            text = lang_entry
        else:
            text = None
        return str(text).strip() if text else None

    def _has_suitability(self, lang_block: Dict[str, Any]) -> bool:
        for time_of_day in self.TIMES:
            entry = lang_block.get(time_of_day)
            if not isinstance(entry, dict) or "suitable" not in entry:
                return False
        return True

    def _request_suitability(self, script_text: str) -> Dict[str, Dict[str, bool]]:
        prompt = self._prompt_check.read_text(encoding="utf-8")
        messages = [
            {"role": "system", "content": prompt},
            {"role": "user", "content": json.dumps(
                {"script": script_text}, ensure_ascii=False)},
        ]
        response = self.llm_client.chat(
            messages,
            model=self.config.ids.get("daily_model", "gpt-5"),
            response_schema=SuitabilityResponse,
        )
        try:
            parsed = self._parse_schema_response(response, SuitabilityResponse)
            data = parsed.model_dump() if isinstance(parsed, BaseModel) else parsed
            return {
                "morning": {"suitable": bool(data.get("morning", {}).get("suitable"))},
                "afternoon": {"suitable": bool(data.get("afternoon", {}).get("suitable"))},
                "night": {"suitable": bool(data.get("night", {}).get("suitable"))},
            }
        except Exception as exc:
            raise RetryableStepError(
                f"Invalid suitability JSON: {exc}") from exc

    def _request_image_prompt(self, script_text: str, time_of_day: str, gender: str) -> Dict[str, Any]:
        prompt = self._prompt_image.read_text(encoding="utf-8")
        payload = {
            "script": script_text,
            "time_of_day": time_of_day,
            "gender": gender,
        }
        messages = [
            {"role": "system", "content": prompt},
            {"role": "user", "content": json.dumps(
                payload, ensure_ascii=False)},
        ]
        response = self.llm_client.chat(
            messages,
            model=self.config.ids.get("daily_model", "gpt-5"),
            response_schema=ImagePromptResponse,
        )
        try:
            parsed = self._parse_schema_response(response, ImagePromptResponse)
            data = parsed.model_dump() if isinstance(parsed, BaseModel) else parsed
            prompt_text = str(data.get("prompt") or "").strip()
            ref_text = data.get("ref")
            return {"prompt": prompt_text, "ref": ref_text.strip() if isinstance(ref_text, str) else None}
        except Exception as exc:
            raise RetryableStepError(
                f"Invalid image prompt JSON: {exc}") from exc

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
        # Handle parsed dict
        if isinstance(response, dict) and "choices" not in response:
            return schema.model_validate(response)
        # Handle raw string
        if isinstance(response, str):
            return schema.model_validate_json(response)
        # Handle normalized OpenAI response with choices/content
        if isinstance(response, dict) and "choices" in response:
            content = self._extract_content(response)
            return schema.model_validate_json(content)
        # Fallback
        raise RetryableStepError("Unexpected LLM response format")

    def _generate_image(self, prompt: str) -> bytes:
        try:
            result = self.image_client.images.generate(
                model="gpt-image-1",
                prompt=prompt,
                size="1024x1536",
                quality="medium",
                output_format="webp",
                output_compression=95,
            )
            # data is a list of objects with attribute b64_json
            data_list = getattr(result, "data", None)
            if not data_list:
                payload = result.model_dump() if hasattr(result, "model_dump") else {}
                data_list = payload.get("data", [])
            if not data_list:
                raise RetryableStepError("Image generation returned empty data")
            first_item = data_list[0]
            b64_value = first_item.b64_json if hasattr(first_item, "b64_json") else first_item.get("b64_json")
            if not b64_value:
                raise RetryableStepError("Image generation response missing b64_json")
            return base64.b64decode(b64_value)
        except Exception as exc:
            raise RetryableStepError(
                f"Image generation failed: {exc}") from exc

    def _build_preview_filename(self, payload: Dict[str, Any], gender: str, language: str, time_of_day: str) -> str:
        cat_pos = int(payload["category"].get("position") or 0)
        sub_pos = int(payload["subcategory"].get("position") or 0)
        coach_pos = int(payload["coach"].get("id") or 0)
        record_pos = int(payload["record"].get("position") or 0)
        gender_code = "w" if gender == "female" else "m"
        lang_code = language.lower()
        return f"{cat_pos}_{sub_pos}_{coach_pos}_{record_pos}_{gender_code}_{lang_code}_{time_of_day}.webp"

    def _persist_allowed(self, record_id: Any, data: Dict[str, Any]) -> None:
        payload = self._format_json(data)
        try:
            (
                self.supabase.table("affirmations_new")
                .update({"allowed_for_daily": payload})
                .eq("id", record_id)
                .execute()
            )
        except Exception as exc:
            raise RetryableStepError(
                f"Failed to update allowed_for_daily for {record_id}: {exc}") from exc


__all__ = ["Step10DailyAffirmations"]
