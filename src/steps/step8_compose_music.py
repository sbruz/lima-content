from __future__ import annotations

import base64
import io
import json
import os
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import httpx
from mutagen import File as MutagenFile
from pydub import AudioSegment

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
class ComposeJob:
    category: Dict[str, Any]
    subcategory: Dict[str, Any]
    coach: Dict[str, Any]


class Step8ComposeMusic(BaseStep):
    NAME = "compose_music"
    ELEVEN_BASE_URL = "https://api.elevenlabs.io/v1"

    def __init__(self, config: Config) -> None:
        super().__init__(config)
        if not config.languages:
            raise FatalStepError("Config must include at least one language")
        self.supabase = get_supabase_client()
        self.languages = [lang.strip().upper() for lang in config.languages if lang.strip()]
        self.audio_dir = Path("export") / "audio"
        self.audio_dir.mkdir(parents=True, exist_ok=True)
        self.music_model_id = config.ids.get("music_model_id", "music_v1")
        self.volume_db = config.audio_mix.music_volume_db
        self.fade_in_ms = config.audio_mix.music_fade_in_ms
        self.fade_out_ms = config.audio_mix.music_fade_out_ms
        self.voice_fade_in_ms = config.audio_mix.voice_fade_in_ms
        self.voice_fade_out_ms = config.audio_mix.voice_fade_out_ms
        self._ready_music_cache: Dict[Any, Dict[str, Set[str]]] = {}
        self._ready_music_lock = threading.Lock()
        self.music_tail_sec = max(0.0, float(config.music_prompt_tail_sec or 0.0))

        api_key = os.getenv("ELEVENLABS_API_KEY")
        if not api_key:
            raise FatalStepError("ELEVENLABS_API_KEY is not configured")
        self.http = httpx.Client(
            base_url=self.ELEVEN_BASE_URL,
            headers={"xi-api-key": api_key},
            timeout=120,
        )

    # --------------------------------------------------------------------- jobs
    def load_jobs(self) -> List[Job]:
        if not self.should_run():
            return []

        coaches = self._fetch_coaches()
        if not coaches:
            logger.warning(
                "[BUSINESS] Skip compose_music | reason=no_coaches",
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
        payload = ComposeJob(
            category=job.payload["category"],
            subcategory=job.payload["subcategory"],
            coach=job.payload["coach"],
        )
        subcategory_id = payload.subcategory["id"]
        coach_id = payload.coach["id"]

        records = self._fetch_affirmations(subcategory_id, coach_id)
        if not records:
            logger.warning(
                "No affirmations to compose music",
                subcategory=subcategory_id,
                coach=payload.coach.get("coach"),
            )
            return

        record_contexts: List[Dict[str, Any]] = []
        total_tasks = 0
        for record in records:
            record_id = record.get("id")
            music_payload = self._parse_music_json(record.get("music"))
            if not music_payload:
                continue
            script = self._parse_script_json(record.get("script"))
            duration_map = self._parse_duration_json(record.get("duration"))
            self._seed_ready_music_cache(record)
            combos = self._count_pending_music_combinations(record_id, music_payload)
            if combos == 0:
                continue
            record_contexts.append(
                {
                    "record": record,
                    "music": music_payload,
                    "script": script,
                    "duration": duration_map,
                }
            )
            total_tasks += combos

        if total_tasks == 0:
            logger.warning(
                "No eligible affirmations for compose music",
                subcategory=subcategory_id,
                coach=payload.coach.get("coach"),
            )
            return

        logger.info(
            "[BUSINESS] Start compose_music | subcategory={} coach={} combos={}",
            subcategory_id,
            payload.coach.get("coach"),
            total_tasks,
        )

        updates: List[Dict[str, Any]] = []
        completed = 0
        for ctx in record_contexts:
            record = ctx["record"]
            record_id = record.get("id")
            duration_map = ctx["duration"]
            music_payload = ctx["music"]
            script = ctx["script"]
            record_changed = False
            self._seed_ready_music_cache(record)

            for language in self.languages:
                language_iso = language.lower()
                for gender in ("female", "male"):
                    if self._is_music_ready(record_id, gender, language):
                        continue
                    prompt_text = self._extract_prompt(music_payload, gender, language)
                    if not prompt_text:
                        continue

                    voice_filename = self._build_voice_filename(
                        payload, record, gender, language_iso
                    )
                    voice_path = self.audio_dir / f"{voice_filename}.mp3"
                    if not voice_path.exists():
                        logger.warning(
                            "Voice file missing for compose music",
                            record_id=record_id,
                            file=str(voice_path),
                        )
                        continue

                    duration_sec = self._resolve_voice_duration(
                        duration_map, voice_filename, voice_path
                    )
                    duration_with_tail = duration_sec + self.music_tail_sec
                    duration_ms = max(int(duration_with_tail * 1000), 1000)

                    request_payload = self._build_music_request(
                        prompt_text,
                        duration_ms,
                    )

                    job_code = self._build_job_code(
                        payload, record, language, gender
                    )
                    logger.info(
                        "[BUSINESS] Music compose request | job={} aff_id={} duration={:.2f}s",
                        job_code,
                        record_id,
                        duration_with_tail,
                    )

                    music_audio = self._request_music(request_payload)
                    final_audio = self._mix_tracks(
                        voice_path,
                        music_audio,
                        duration_ms,
                    )

                    final_filename = f"{voice_filename}_final.mp3"
                    final_path = self.audio_dir / final_filename
                    final_audio.export(final_path, format="mp3")

                    final_duration = round(len(final_audio) / 1000.0, 3)
                    duration_map = self._update_duration_map(
                        duration_map, final_filename, final_duration
                    )
                    self._mark_ready_music(record_id, gender, language)
                    record_changed = True

                    completed += 1
                    logger.info(
                        "[BUSINESS] Music composed | job={} aff_id={} file={} duration={:.2f}s done={}/{}",
                        job_code,
                        record_id,
                        final_filename,
                        final_duration,
                        completed,
                        total_tasks,
                    )
                    logger.info(
                        "[PROG] Compose progress | done={}/{}",
                        completed,
                        total_tasks,
                    )

            if record_changed:
                updates.append(
                    {
                        "id": record_id,
                        "duration": duration_map,
                    }
                )

        if updates:
            self._persist_durations(updates)

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
                .select("id, position, script, duration, music, ready_music")
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
        ready_records = [record for record in records if record.get("music")]
        filtered = filter_by_range(
            ready_records,
            tuple(self.config.range.positions),
        )
        return filtered

    def _parse_music_json(self, raw: Any) -> Dict[str, Any]:
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except json.JSONDecodeError:
                return {}
        return raw if isinstance(raw, dict) else {}

    def _parse_script_json(self, raw: Any) -> Dict[str, Any]:
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except json.JSONDecodeError:
                return {}
        return raw if isinstance(raw, dict) else {}

    def _parse_duration_json(self, raw: Any) -> Dict[str, Any]:
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except json.JSONDecodeError:
                return {}
        return raw if isinstance(raw, dict) else {}

    def _seed_ready_music_cache(self, record: Dict[str, Any]) -> None:
        record_id = record.get("id")
        if record_id is None:
            return
        with self._ready_music_lock:
            if record_id in self._ready_music_cache:
                return
            state = self._parse_ready_map(record.get("ready_music"))
            self._ready_music_cache[record_id] = state

    def _empty_ready_state(self) -> Dict[str, Set[str]]:
        return {"female": set(), "male": set()}

    def _parse_ready_map(self, raw: Any) -> Dict[str, Set[str]]:
        state = self._empty_ready_state()
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except json.JSONDecodeError:
                raw = {}
        if isinstance(raw, dict):
            for gender in ("female", "male"):
                entries = raw.get(gender)
                if isinstance(entries, list):
                    state[gender].update(str(item).upper() for item in entries)
        return state

    def _format_ready_map(self, state: Dict[str, Set[str]]) -> Dict[str, List[str]]:
        return {
            "female": sorted({item.upper() for item in state.get("female", set())}),
            "male": sorted({item.upper() for item in state.get("male", set())}),
        }

    def _count_pending_music_combinations(
        self, record_id: Any, music_payload: Dict[str, Any]
    ) -> int:
        combos = 0
        if not isinstance(music_payload, dict):
            return combos
        state = self._ready_music_cache.get(record_id, self._empty_ready_state())
        for language in self.languages:
            for gender in ("female", "male"):
                if language in state.get(gender, set()):
                    continue
                prompt = self._extract_prompt(music_payload, gender, language)
                if isinstance(prompt, str) and prompt.strip():
                    combos += 1
        return combos

    def _extract_prompt(
        self, music_payload: Dict[str, Any], gender: str, language: str
    ) -> Optional[str]:
        block = music_payload.get(gender)
        if not isinstance(block, dict):
            return None
        entry = block.get(language)
        if isinstance(entry, dict):
            prompt = entry.get("prompt")
            if prompt:
                return str(prompt).strip()
        elif isinstance(entry, str):
            return entry.strip()
        return None

    def _build_voice_filename(
        self,
        payload: ComposeJob,
        record: Dict[str, Any],
        gender: str,
        language_iso: str,
    ) -> str:
        cat_pos = int(payload.category.get("position") or 0)
        sub_pos = int(payload.subcategory.get("position") or 0)
        coach_id = payload.coach.get("id")
        record_pos = int(record.get("position") or 0)
        gender_code = "w" if gender == "female" else "m"
        return f"{cat_pos}_{sub_pos}_{coach_id}_{record_pos}_{gender_code}_{language_iso}"

    def _is_music_ready(self, record_id: Any, gender: str, language: str) -> bool:
        with self._ready_music_lock:
            state = self._ready_music_cache.get(record_id)
            if not state:
                return False
            return language.upper() in state.get(gender, set())

    def _mark_ready_music(self, record_id: Any, gender: str, language: str) -> None:
        lang = language.upper()
        with self._ready_music_lock:
            state = self._ready_music_cache.setdefault(record_id, self._empty_ready_state())
            if lang in state[gender]:
                return
            state[gender].add(lang)
            payload = self._format_ready_map(state)
        self._persist_ready_field(record_id, payload, "ready_music")

    def _persist_ready_field(
        self, record_id: Any, payload: Dict[str, List[str]], field_name: str
    ) -> None:
        normalized = json.loads(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        try:
            (
                self.supabase.table("affirmations_new")
                .update({field_name: normalized})
                .eq("id", record_id)
                .execute()
            )
        except Exception as exc:  # pragma: no cover
            raise RetryableStepError(
                f"Failed to update {field_name} for {record_id}: {exc}"
            ) from exc

    def _resolve_voice_duration(
        self,
        duration_map: Dict[str, Any],
        filename: str,
        voice_path: Path,
    ) -> float:
        if filename in duration_map:
            try:
                return float(duration_map[filename])
            except (TypeError, ValueError):
                pass
        try:
            audio_file = MutagenFile(str(voice_path))
            if audio_file and getattr(audio_file, "info", None):
                length = getattr(audio_file.info, "length", None)
                if length:
                    return float(length)
        except Exception as exc:  # pragma: no cover
            logger.warning("Failed to read voice duration", error=str(exc))
        # fallback to pydub length
        try:
            voice = AudioSegment.from_file(voice_path, format="mp3")
            return len(voice) / 1000.0
        except Exception as exc:  # pragma: no cover
            logger.warning("Failed to load voice file", error=str(exc))
        return 120.0

    def _build_music_request(self, prompt: str, duration_ms: int) -> Dict[str, Any]:
        return {
            "prompt": prompt,
            "music_length_ms": duration_ms,
            "model_id": self.music_model_id,
            "force_instrumental": True,
        }

    def _request_music(self, payload: Dict[str, Any]) -> AudioSegment:
        try:
            response = self.http.post("/music", json=payload)
            response.raise_for_status()
        except httpx.HTTPError as exc:
            logger.error("Music compose API failed", payload=payload, error=str(exc))
            raise RetryableStepError(f"ElevenLabs music request failed: {exc}") from exc

        content_type = response.headers.get("content-type", "")
        if "application/json" in content_type:
            data = response.json()
            audio_b64 = (
                data.get("audio_base64")
                or data.get("audio")
                or data.get("audio_data")
            )
            if not audio_b64:
                raise RetryableStepError("ElevenLabs music response missing audio")

            if audio_b64.startswith("data:"):
                audio_b64 = audio_b64.split(",", 1)[-1]
            try:
                audio_bytes = base64.b64decode(audio_b64)
            except Exception as exc:  # pragma: no cover
                raise RetryableStepError(f"Invalid music audio payload: {exc}") from exc
        else:
            audio_bytes = response.content
            if not audio_bytes:
                raise RetryableStepError("Empty music response body")

        return AudioSegment.from_file(io.BytesIO(audio_bytes), format="mp3")

    def _mix_tracks(
        self,
        voice_path: Path,
        music_segment: AudioSegment,
        target_duration_ms: int,
    ) -> AudioSegment:
        voice = AudioSegment.from_file(voice_path, format="mp3")

        if self.voice_fade_in_ms:
            voice = voice.fade_in(self.voice_fade_in_ms)
        if self.voice_fade_out_ms:
            voice = voice.fade_out(self.voice_fade_out_ms)

        if self.volume_db:
            music_segment = music_segment.apply_gain(self.volume_db)
        if self.fade_in_ms:
            music_segment = music_segment.fade_in(self.fade_in_ms)
        if self.fade_out_ms:
            music_segment = music_segment.fade_out(self.fade_out_ms)

        music_segment = self._match_duration(music_segment, target_duration_ms)
        final = music_segment.overlay(voice)
        return final

    def _match_duration(self, music: AudioSegment, target_ms: int) -> AudioSegment:
        if len(music) >= target_ms:
            return music[:target_ms]
        repeats = target_ms // len(music) + 1
        extended = music * repeats
        return extended[:target_ms]

    def _update_duration_map(
        self,
        duration_map: Dict[str, Any],
        filename: str,
        duration_sec: float,
    ) -> Dict[str, Any]:
        current = dict(duration_map or {})
        current[filename] = duration_sec
        return current

    def _persist_durations(self, updates: List[Dict[str, Any]]) -> None:
        try:
            for update in updates:
                (
                    self.supabase.table("affirmations_new")
                    .update({"duration": self._format_json(update["duration"])} )
                    .eq("id", update["id"])
                    .execute()
                )
        except Exception as exc:
            raise RetryableStepError(f"Failed to persist durations: {exc}") from exc

    def _build_job_code(
        self,
        payload: ComposeJob,
        record: Dict[str, Any],
        language: str,
        gender: str,
    ) -> str:
        cat_pos = int(payload.category.get("position") or 0)
        sub_pos = int(payload.subcategory.get("position") or 0)
        coach_id = payload.coach.get("id")
        record_pos = int(record.get("position") or 0)
        gender_code = gender[0].upper()
        lang_code = language.upper()
        return f"C{cat_pos}-S{sub_pos}-N{coach_id}-P{record_pos}-{lang_code}-{gender_code}"

    def _format_json(self, data: Any) -> Any:
        return json.loads(json.dumps(data, ensure_ascii=False, sort_keys=True))
