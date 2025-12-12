from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional

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


class Step99ExportData(BaseStep):
    NAME = "export_data"
    GENDERS = ("female", "male")
    TIMES = ("morning", "afternoon", "night")
    EXPORT_DIR = Path("export")
    PREVIEW_DIR = EXPORT_DIR / "daily_previews"

    def __init__(self, config: Config) -> None:
        super().__init__(config)
        if not config.languages:
            raise FatalStepError("Config must include at least one language")
        self.supabase = get_supabase_client()
        self.languages = [lang.strip().upper() for lang in config.languages if lang.strip()]
        self._dataset: List[Dict[str, Any]] = []
        try:
            self.EXPORT_DIR.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise FatalStepError(f"Failed to prepare export directory: {exc}") from exc

    # --------------------------------------------------------------------- jobs
    def load_jobs(self) -> List[Job]:
        if not self.should_run():
            return []

        self._dataset = self._build_dataset()
        if not self._dataset:
            logger.warning("[BUSINESS] Export data skipped | reason=no_data")
            return []

        jobs: List[Job] = []
        for language in self.languages:
            jobs.append(make_job(self.NAME, {"language": language}, key_fields=["language"]))
        return jobs

    # ------------------------------------------------------------------ process
    def process(self, job: Job) -> None:
        language = job.payload["language"]
        total_categories = len(self._dataset)
        logger.info(
            "[BUSINESS] Export start | lang={} categories={}",
            language,
            total_categories,
        )
        target_file = self.EXPORT_DIR / f"content_{language.lower()}.db"
        if target_file.exists():
            target_file.unlink()
        conn = sqlite3.connect(target_file)
        try:
            self._init_schema(conn)
            inserted = self._export_language(conn, language, total_categories)
            conn.commit()
        except Exception as exc:
            logger.exception("[BUSINESS] Export failed", lang=language, error=str(exc))
            raise
        finally:
            conn.close()
        logger.info(
            "[BUSINESS] Export completed | lang={} file={} categories={} affirmations={}",
            language,
            target_file.name,
            total_categories,
            inserted,
        )

    # ----------------------------------------------------------------- helpers
    def _build_dataset(self) -> List[Dict[str, Any]]:
        dataset: List[Dict[str, Any]] = []
        categories = filter_by_range(self._fetch_categories(), tuple(self.config.range.categories))
        coaches = self._fetch_coaches()
        if not coaches:
            return []
        for category in categories:
            subcategories = filter_by_range(
                self._fetch_subcategories(category["id"]),
                tuple(self.config.range.subcategories),
            )
            parsed_cat_loc = self._parse_json(category.get("localization"))
            cat_entry = {
                "id": category["id"],
                "position": category.get("position"),
                "localization": parsed_cat_loc,
                "subcategories": [],
            }
            for subcategory in subcategories:
                parsed_sub_loc = self._parse_json(subcategory.get("localization"))
                sub_entry = {
                    "id": subcategory["id"],
                    "category_id": subcategory.get("category_id"),
                    "position": subcategory.get("position"),
                    "localization": parsed_sub_loc,
                    "shadow_w": subcategory.get("shadow_w"),
                    "shadow_m": subcategory.get("shadow_m"),
                    "views": subcategory.get("views"),
                    "is_daily_suitable": subcategory.get("is_daily_suitable"),
                    "coaches": [],
                }
                for coach in coaches:
                    records = filter_by_range(
                        self._fetch_affirmations(subcategory["id"], coach["id"]),
                        tuple(self.config.range.positions),
                    )
                    if not records:
                        continue
                    coach_entry = {
                        "id": coach["id"],
                        "position": coach.get("position"),
                        "coach": coach.get("coach"),
                        "coach_name": coach.get("coach_name"),
                        "description": self._parse_json(coach.get("coach_UI_description")),
                    "records": [
                        {
                            "id": record["id"],
                            "position": record.get("position"),
                            "script": self._parse_json(record.get("script")),
                            "popular": self._parse_json(record.get("popular_aff")),
                            "banners": self._parse_json(record.get("aff_for_banners")),
                        }
                        for record in records
                    ],
                }
                sub_entry["coaches"].append(coach_entry)
                if sub_entry["coaches"]:
                    cat_entry["subcategories"].append(sub_entry)
            if cat_entry["subcategories"]:
                dataset.append(cat_entry)
        return dataset

    def _init_schema(self, conn: sqlite3.Connection) -> None:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE categories (
                id INTEGER PRIMARY KEY,
                position INTEGER,
                name TEXT NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE subcategories (
                id INTEGER PRIMARY KEY,
                position INTEGER,
                name TEXT NOT NULL,
                shadow_w TEXT NOT NULL,
                shadow_m TEXT NOT NULL,
                views INTEGER NOT NULL,
                is_daily_suitable INTEGER NOT NULL,
                category_id INTEGER NOT NULL,
                FOREIGN KEY(category_id) REFERENCES categories(id)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE coaches (
                id INTEGER PRIMARY KEY,
                position INTEGER,
                name TEXT NOT NULL,
                description TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE affirmations (
                sub_id INTEGER NOT NULL,
                coach_id INTEGER NOT NULL,
                position INTEGER,
                gender INTEGER NOT NULL,
                title TEXT NOT NULL,
                subtitle TEXT NOT NULL,
                script TEXT NOT NULL,
                morning_aff TEXT,
                afternoon_aff TEXT,
                evening_aff TEXT,
                is_morning INTEGER NOT NULL,
                is_afternoon INTEGER NOT NULL,
                is_night INTEGER NOT NULL,
                FOREIGN KEY(sub_id) REFERENCES subcategories(id),
                FOREIGN KEY(coach_id) REFERENCES coaches(id)
            )
            """
        )

    def _export_language(self, conn: sqlite3.Connection, language: str, total_categories: int) -> int:
        cur = conn.cursor()
        inserted_affirmations = 0
        inserted_categories: set[int] = set()
        inserted_subcategories: set[int] = set()
        inserted_coaches: set[int] = set()
        processed_cats = 0
        for category in self._dataset:
            cat_title = self._get_category_title(category["localization"], language)
            if not cat_title:
                continue
            category_used = False
            cat_id = category["id"]
            if cat_id not in inserted_categories:
                cur.execute(
                    "INSERT INTO categories (id, position, name) VALUES (?, ?, ?)",
                    (cat_id, category.get("position"), cat_title),
                )
                inserted_categories.add(cat_id)
            for subcategory in category["subcategories"]:
                sub_name = self._get_subcategory_name(subcategory["localization"], language)
                if not sub_name:
                    continue
                sub_id = subcategory["id"]
                if sub_id not in inserted_subcategories:
                    cur.execute(
                        """
                        INSERT INTO subcategories (
                            id, position, name, shadow_w, shadow_m, views, is_daily_suitable, category_id
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            sub_id,
                            subcategory.get("position"),
                            sub_name,
                            subcategory.get("shadow_w"),
                            subcategory.get("shadow_m"),
                            subcategory.get("views"),
                            self._compute_daily_suitable(subcategory),
                            cat_id,
                        ),
                    )
                    inserted_subcategories.add(sub_id)
                for coach in subcategory["coaches"]:
                    coach_id = coach["id"]
                    coach_name = coach.get("coach_name") or coach.get("coach") or ""
                    coach_desc = self._get_coach_description(coach["description"], language)
                    if coach_id not in inserted_coaches:
                        cur.execute(
                            "INSERT INTO coaches (id, position, name, description) VALUES (?, ?, ?, ?)",
                            (coach_id, coach.get("position"), coach_name, coach_desc),
                        )
                        inserted_coaches.add(coach_id)
                    added = self._insert_affirmations(
                        cur,
                        category,
                        subcategory,
                        coach,
                        sub_id,
                        coach_id,
                        language,
                    )
                    if added:
                        inserted_affirmations += added
                        category_used = True
            if category_used:
                processed_cats += 1
                logger.info(
                    "[BUSINESS] Export progress | lang={} cat={}/{} aff={}",
                    language,
                    processed_cats,
                    total_categories,
                    inserted_affirmations,
                )
        return inserted_affirmations

    def _insert_affirmations(
        self,
        cur: sqlite3.Cursor,
        category: Dict[str, Any],
        subcategory: Dict[str, Any],
        coach: Dict[str, Any],
        sub_id: Any,
        coach_id: Any,
        language: str,
    ) -> int:
        count = 0
        cat_pos = category.get("position")
        sub_pos = subcategory.get("position")
        for record in coach["records"]:
            for gender in self.GENDERS:
                try:
                    entry = self._extract_script_entry(
                        record["script"],
                        record.get("popular"),
                        record.get("banners"),
                        gender,
                        language,
                        category,
                        subcategory,
                        coach,
                    )
                    if not entry:
                        continue
                    record_pos = record.get("position")
                    is_flags = {
                        time_of_day: int(
                            self._has_preview(cat_pos, sub_pos, coach_id, record_pos, gender, language, time_of_day)
                        )
                        for time_of_day in self.TIMES
                    }
                    gender_flag = 1 if gender == "male" else 0
                    cur.execute(
                        """
                        INSERT INTO affirmations (
                            sub_id, coach_id, position, gender, title, subtitle, script,
                            morning_aff, afternoon_aff, evening_aff,
                            is_morning, is_afternoon, is_night
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            sub_id,
                            coach_id,
                            record_pos,
                            gender_flag,
                            entry["title"],
                            entry["popular_aff"],
                            entry["script"],
                            entry.get("morning_aff"),
                            entry.get("afternoon_aff"),
                            entry.get("evening_aff"),
                            is_flags["morning"],
                            is_flags["afternoon"],
                            is_flags["night"],
                        ),
                    )
                    count += 1
                except Exception as exc:
                    logger.exception(
                        "[BUSINESS] Export entry failed",
                        cat_pos=cat_pos,
                        sub_pos=sub_pos,
                        sub_id=sub_id,
                        coach_id=coach_id,
                        record_id=record.get("id"),
                        gender=gender,
                        language=language,
                        error=str(exc),
                    )
                    raise
        return count

    def _has_preview(
        self,
        cat_pos: Any,
        sub_pos: Any,
        coach_id: Any,
        record_pos: Any,
        gender: str,
        language: str,
        time_of_day: str,
    ) -> bool:
        gender_code = "w" if gender == "female" else "m"
        filename = (
            f"{int(cat_pos or 0)}_{int(sub_pos or 0)}_{int(coach_id or 0)}_{int(record_pos or 0)}_"
            f"{gender_code}_{language.lower()}_{time_of_day}.webp"
        )
        return (self.PREVIEW_DIR / filename).exists()

    def _get_category_title(self, localization: Dict[str, Any], language: str) -> Optional[str]:
        if not isinstance(localization, dict):
            return None
        value = localization.get(language)
        return str(value).strip() if value else None

    def _get_subcategory_name(self, localization: Dict[str, Any], language: str) -> Optional[str]:
        if not isinstance(localization, dict):
            return None
        for gender in self.GENDERS:
            block = localization.get(gender)
            if not isinstance(block, dict):
                continue
            title_block = block.get("title")
            if isinstance(title_block, dict):
                value = title_block.get(language)
            else:
                value = title_block
            if value:
                text = str(value).strip()
                if text:
                    return text
        return None

    def _get_coach_description(self, localization: Dict[str, Any], language: str) -> Optional[str]:
        if not isinstance(localization, dict):
            return None
        value = localization.get(language)
        return str(value).strip() if value else None

    def _compute_daily_suitable(self, subcategory: Dict[str, Any]) -> int:
        if "is_daily_suitable" not in subcategory:
            logger.error(
                "[BUSINESS] Export fatal | missing is_daily_suitable | sub_id={} cat_id={} payload_keys={} query_select={}",
                subcategory.get("id"),
                subcategory.get("category_id"),
                sorted(subcategory.keys()),
                "id, category_id, position, localization, shadow_w, shadow_m, views, is_daily_suitable",
            )
            raise FatalStepError(
                f"Field is_daily_suitable is missing in subcategories payload | sub_id={subcategory.get('id')} "
                f"cat_id={subcategory.get('category_id')}"
            )
        value = subcategory.get("is_daily_suitable")
        return 1 if value is True or value is None else 0

    def _extract_script_entry(
        self,
        script_map: Dict[str, Any],
        popular_map: Dict[str, Any],
        banners_map: Optional[Dict[str, Any]],
        gender: str,
        language: str,
        category: Dict[str, Any],
        subcategory: Dict[str, Any],
        coach: Dict[str, Any],
    ) -> Optional[Dict[str, str]]:
        if not isinstance(script_map, dict):
            return None
        gender_block = script_map.get(gender)
        if not isinstance(gender_block, dict):
            return None
        lang_entry = gender_block.get(language)
        if not isinstance(lang_entry, dict):
            return None
        title = (lang_entry.get("title") or "").strip()
        script = (lang_entry.get("script") or "").strip()
        popular_line = self._extract_popular_line(popular_map, gender, language)
        if not title or not script:
            return None
        if not popular_line:
            logger.error(
                "[BUSINESS] Export skip | reason=no_popular cat={} sub={} sub_id={} coach={} gender={} lang={}",
                category.get("position"),
                subcategory.get("position"),
                subcategory.get("id"),
                coach.get("coach"),
                gender,
                language,
            )
            return None
        return {
            "title": title,
            "script": script,
            "popular_aff": popular_line,
            "morning_aff": self._extract_time_aff(banners_map, gender, language, "morning"),
            "afternoon_aff": self._extract_time_aff(banners_map, gender, language, "afternoon"),
            "evening_aff": self._extract_time_aff(banners_map, gender, language, "late evening"),
        }

    def _extract_popular_line(self, popular_map: Dict[str, Any], gender: str, language: str) -> Optional[str]:
        if not isinstance(popular_map, dict):
            return None
        gender_block = popular_map.get(gender)
        if not isinstance(gender_block, dict):
            return None
        line = gender_block.get(language)
        return str(line).strip() if line else None

    def _extract_time_aff(
        self,
        banners_map: Optional[Dict[str, Any]],
        gender: str,
        language: str,
        time_of_day: str,
    ) -> Optional[str]:
        if not isinstance(banners_map, dict):
            return None
        gender_block = banners_map.get(gender)
        if not isinstance(gender_block, dict):
            return None
        lang_block = gender_block.get(language)
        if not isinstance(lang_block, dict):
            return None
        value = lang_block.get(time_of_day)
        return str(value).strip() if value else None

    def _fetch_categories(self) -> List[Dict[str, Any]]:
        try:
            response = self.supabase.table("categories").select("id, position, localization").order("position").execute()
        except Exception as exc:
            raise FatalStepError(f"Failed to load categories: {exc}") from exc
        return getattr(response, "data", []) or []

    def _fetch_subcategories(self, category_id: Any) -> List[Dict[str, Any]]:
        try:
            response = (
                self.supabase.table("subcategories")
                .select("id, category_id, position, localization, shadow_w, shadow_m, views, is_daily_suitable")
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
                .select("id, coach, coach_name, coach_UI_description, position")
                .in_("coach", desired)
                .order("position")
                .execute()
            )
        except Exception as exc:
            raise FatalStepError(f"Failed to load coaches: {exc}") from exc
        data = getattr(response, "data", []) or []
        return data

    def _fetch_affirmations(self, subcategory_id: Any, coach_id: Any) -> List[Dict[str, Any]]:
        try:
            response = (
                self.supabase.table("affirmations_new")
                .select("id, position, script, popular_aff, aff_for_banners")
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


__all__ = ["Step99ExportData"]
