from __future__ import annotations

import random
from dataclasses import dataclass
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


@dataclass(frozen=True)
class ViewsJobPayload:
    category_id: int
    category_position: Optional[int]
    category_order: int
    subcategories: List[Dict[str, Any]]


class Step3FillViews(BaseStep):
    NAME = "fill_views"

    def __init__(self, config: Config) -> None:
        super().__init__(config)
        self.supabase = get_supabase_client()
        self.settings = config.views

    def load_jobs(self) -> List[Job]:
        if not self.should_run():
            return []

        categories = self._fetch_categories()
        filtered_categories = filter_by_range(
            categories, tuple(self.config.range.categories)
        )

        jobs: List[Job] = []
        for order_idx, category in enumerate(filtered_categories, start=1):
            subcategories = self._fetch_subcategories(category["id"])
            filtered_subcats = filter_by_range(
                subcategories, tuple(self.config.range.subcategories)
            )
            if not filtered_subcats:
                continue
            payload = {
                "category_id": category["id"],
                "category": {
                    "id": category["id"],
                    "name": category.get("name"),
                    "position": category.get("position"),
                    "order": order_idx,
                },
                "subcategories": [
                    {
                        "id": sub["id"],
                        "position": sub.get("position"),
                        "order": idx,
                    }
                    for idx, sub in enumerate(filtered_subcats, start=1)
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
        category = job.payload["category"]
        subcategories = job.payload["subcategories"]
        total = len(subcategories)

        if total == 0:
            logger.info(
                "[BUSINESS] Skip fill views | category={} reason=no_subcategories",
                category["id"],
            )
            return

        logger.info(
            "[BUSINESS] Start fill views | category={} total={}",
            category["id"],
            total,
        )

        cat_position = self._resolve_position(
            category.get("position"), category.get("order")
        )

        rng = self._build_rng(category["id"])

        updates: List[Dict[str, Any]] = []
        view_values: List[int] = []
        for sub in subcategories:
            sub_id = sub["id"]
            sub_position = self._resolve_position(
                sub.get("position"), sub.get("order")
            )
            views_value = self._compute_views(cat_position, sub_position, rng)
            view_values.append(views_value)
            updates.append({"id": sub_id, "views": views_value})

        self._persist_views(updates)

        min_views = min(view_values)
        max_views = max(view_values)
        avg_views = sum(view_values) / len(view_values)

        logger.info(
            "[BUSINESS] Views updated | category={} count={} min={} max={} avg={:.1f}",
            category["id"],
            total,
            min_views,
            max_views,
            avg_views,
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
                .select("id, position")
                .eq("category_id", category_id)
                .order("position")
                .execute()
            )
        except Exception as exc:
            raise FatalStepError(
                f"Failed to load subcategories for category {category_id}: {exc}"
            ) from exc
        return getattr(response, "data", []) or []

    def _build_rng(self, category_id: int) -> random.Random:
        if self.settings.seed is not None:
            seed_value = self.settings.seed + category_id
            return random.Random(seed_value)
        return random.Random()

    def _compute_views(
        self, category_position: int, subcategory_position: int, rng: random.Random
    ) -> int:
        base_min = max(self.settings.base_min, 0)
        base_max = max(self.settings.base_max, 0)
        cat_term = (10 - category_position) * base_min
        sub_term = (subcategory_position - 10) * 100
        lower = max(cat_term + sub_term, 0)
        upper = lower + base_max
        if upper < lower:
            upper = lower
        return rng.randint(int(lower), int(upper))

    def _persist_views(self, updates: List[Dict[str, Any]]) -> None:
        try:
            for update in updates:
                (
                    self.supabase.table("subcategories")
                    .update({"views": update["views"]})
                    .eq("id", update["id"])
                    .execute()
                )
        except Exception as exc:
            raise RetryableStepError(f"Failed to update views: {exc}") from exc

    @staticmethod
    def _resolve_position(value: Optional[int], fallback: Optional[int]) -> int:
        if value is None:
            if fallback is None:
                return 1
            return int(fallback)
        return int(value)


__all__ = ["Step3FillViews"]
