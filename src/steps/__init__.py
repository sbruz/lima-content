from __future__ import annotations

from typing import List

from src.config import Config

from .step1_localize_categories import Step1LocalizeCategories
from .step2_localize_subcategories import Step2LocalizeSubcategories
from .step3_fill_views import Step3FillViews
from .step4_generate_affirmations import Step4GenerateAffirmations
from .step5_script_affirmations import Step5ScriptAffirmations
from .step6_generate_audio import Step6GenerateAudio
from .step7_music_prompts import Step7MusicPrompts
from .step8_compose_music import Step8ComposeMusic
from .step9_make_affirmations import Step9MakeAffirmations
from .step10_daily_affirmations import Step10DailyAffirmations
from .step11_popular_affirmations import Step11PopularAffirmations
from .step12_gentle_affirmations import Step12CoachAffirmationForTimeOfDay
from .step99_export_data import Step99ExportData


def build_steps(config: Config) -> List:
    """Instantiate pipeline steps in execution order."""
    return [
        Step1LocalizeCategories(config),
        Step2LocalizeSubcategories(config),
        Step3FillViews(config),
        Step4GenerateAffirmations(config),
        Step5ScriptAffirmations(config),
        Step6GenerateAudio(config),
        Step7MusicPrompts(config),
        Step8ComposeMusic(config),
        Step9MakeAffirmations(config),
        Step10DailyAffirmations(config),
        Step11PopularAffirmations(config),
        Step12CoachAffirmationForTimeOfDay(config),
        Step99ExportData(config),
    ]


__all__ = ["build_steps"]
