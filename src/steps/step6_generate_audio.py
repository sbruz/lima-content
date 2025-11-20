from __future__ import annotations

import base64
import io
import json
import os
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from datetime import datetime

try:  # pragma: no cover - import guard
    from elevenlabs.client import ElevenLabs
    from elevenlabs.play import play
except ImportError as exc:  # pragma: no cover - handled later
    ElevenLabs = None  # type: ignore
    play = None  # type: ignore
    ELEVENLABS_IMPORT_ERROR = exc
else:
    ELEVENLABS_IMPORT_ERROR = None

from mutagen import File as MutagenFile

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
from src.utils.llm_client import LLMClient


@dataclass(frozen=True)
class LanguagePlan:
    code: str  # uppercase config code (matches scripts)
    iso: str   # lowercase ISO 639-1 for ElevenLabs


class Step6GenerateAudio(BaseStep):
    NAME = "generate_audio"
    MODEL_ID = "eleven_v3"
    STABILITY = 0.5
    EXPORT_DIR = Path("export") / "audio"
    MAX_AUDIO_DURATION_SEC = 300.0

    def __init__(self, config: Config) -> None:
        super().__init__(config)
        if ELEVENLABS_IMPORT_ERROR is not None:
            raise FatalStepError(
                "Package 'elevenlabs' is not installed.\n"
                "Install it via requirements.txt and retry."
            ) from ELEVENLABS_IMPORT_ERROR
        if not config.languages:
            raise FatalStepError("Config must include at least one language")
        self.supabase = get_supabase_client()
        self.client = ElevenLabs(api_key=os.getenv("ELEVENLABS_API_KEY"))
        self.preview_audio = os.getenv("LIMA_TTS_PREVIEW", "0") == "1"
        env_stub = os.getenv("LIMA_TTS_STUB", "0") == "1"
        self.stub_mode = bool(config.generate_audio_stub) or env_stub
        self.languages = self._select_languages()
        self._duration_cache: Dict[Any, Dict[str, float]] = {}
        self._duration_reset: set[Any] = set()
        self._duration_lock = threading.Lock()
        self._ready_voice_cache: Dict[Any, Dict[str, Set[str]]] = {}
        self._ready_voice_lock = threading.Lock()
        try:
            self.EXPORT_DIR.mkdir(parents=True, exist_ok=True)
        except OSError as exc:  # pragma: no cover - filesystem guard
            raise FatalStepError(
                f"Cannot create export directory {self.EXPORT_DIR}: {exc}"
            ) from exc

    # --------------------------------------------------------------------- jobs
    def load_jobs(self) -> List[Job]:
        if not self.should_run():
            return []

        categories = self._fetch_categories()
        filtered_categories = filter_by_range(
            categories, tuple(self.config.range.categories)
        )

        coaches = self._fetch_coaches()
        if not coaches:
            logger.warning(
                "[BUSINESS] Skip generate_audio | reason=no_coaches",
                versions=self.config.versions,
            )
            return []

        jobs: List[Job] = []
        for category in filtered_categories:
            subcategories = self._fetch_subcategories(category["id"])
            filtered_subcategories = filter_by_range(
                subcategories, tuple(self.config.range.subcategories)
            )
            for subcategory in filtered_subcategories:
                for coach in coaches:
                    records = self._fetch_affirmations(
                        subcategory["id"], coach["id"]
                    )
                    filtered_records = filter_by_range(
                        records, tuple(self.config.range.positions)
                    )
                    for record in filtered_records:
                        if not record.get("script"):
                            logger.warning(
                                "Skipping affirmation without script",
                                subcategory=subcategory["id"],
                                coach=coach["coach"],
                                position=record.get("position"),
                            )
                            continue
                        self._seed_duration_cache(record)
                        self._seed_ready_voice_cache(record)
                        ready_state = self._ready_voice_cache.get(
                            record.get("id"), self._empty_ready_state()
                        )
                        for language in self.languages:
                            need_language = False
                            for gender in ("female", "male"):
                                if language.code not in ready_state.get(gender, set()):
                                    need_language = True
                                    break
                            if not need_language:
                                continue
                            payload = {
                                "category_id": category["id"],
                                "category_position": category.get("position"),
                                "subcategory_id": subcategory["id"],
                                "subcategory_position": subcategory.get("position"),
                                "coach_id": coach["id"],
                                "coach": coach.get("coach"),
                                "voice_id": coach.get("voice_id"),
                                "record_id": record.get("id"),
                                "record_position": record.get("position"),
                                "language": language.code,
                                "language_iso": language.iso,
                                "record": {
                                    "id": record.get("id"),
                                    "position": record.get("position"),
                                    "script": record.get("script"),
                                    "ready_voice": record.get("ready_voice"),
                                },
                            }
                            jobs.append(
                                make_job(
                                    self.NAME,
                                    payload,
                                    key_fields=[
                                        "category_id",
                                        "subcategory_id",
                                        "coach_id",
                                        "record_id",
                                        "language",
                                    ],
                                )
                            )
        return jobs

    # ------------------------------------------------------------------ process
    def process(self, job: Job) -> None:
        language = job.payload["language"]
        language_iso = job.payload["language_iso"]
        record = job.payload["record"]
        record_id = record["id"]
        voice_id = job.payload.get("voice_id")
        if not voice_id:
            raise FatalStepError(
                f"Coach {job.payload.get('coach')} is missing voice_EL_ID"
            )
        self._seed_ready_voice_cache(record)
        script_payload = self._parse_script(record.get("script"))
        if not script_payload:
            logger.warning(
                "No script payload for record",
                record_id=record_id,
                language=language,
            )
            return

        self._reset_duration_if_needed(record_id)

        total_segments = 0
        available_segments: List[Tuple[str, Dict[str, str]]] = []
        job_code = (
            f"C{job.payload.get('category_position')}-"
            f"S{job.payload.get('subcategory_position')}-"
            f"N{job.payload.get('coach_id')}-"
            f"P{job.payload.get('record_position')}-"
            f"{language}"
        )
        for gender in ("female", "male"):
            if self._is_voice_ready(record_id, gender, language):
                continue
            block = script_payload.get(gender)
            if not isinstance(block, dict):
                logger.warning(
                    "Script gender block missing",
                    record_id=record_id,
                    gender=gender,
                    block_type=type(block).__name__,
                )
                return
            lang_entry_raw = block.get(language)
            if not lang_entry_raw:
                logger.warning(
                    "Script missing language entry",
                    record_id=record_id,
                    gender=gender,
                    language=language,
                    available=list(block.keys()),
                )
                continue
            lang_entry = self._normalize_lang_entry(lang_entry_raw)
            if lang_entry and lang_entry.get("script"):
                total_segments += 1
                available_segments.append((gender, lang_entry))
            else:
                logger.warning(
                    "Script text empty",
                    record_id=record_id,
                    gender=gender,
                    language=language,
                    entry=lang_entry,
                )

        if not available_segments:
            logger.info(
                "[BUSINESS] Skip generate_audio | reason=already_ready",
                job=job_code,
                record_id=record_id,
                language=language,
            )
            return

        completed = 0
        for gender, lang_entry in available_segments:
            text = str(lang_entry.get("script", "")).strip()
            if not text:
                logger.warning(
                    "Empty script text",
                    record_id=record_id,
                    language=language,
                    gender=gender,
                )
                continue

            logger.info(
                "[BUSINESS] Audio request | job={}-{} aff_id={} chars={} A{}",
                job_code,
                gender[0].upper(),
                record_id,
                len(text),
                job.payload.get("_attempt", 1),
            )
            if self.stub_mode:
                snippet = text[:200].replace("\n", " ")
                logger.info(
                    "[STUB] ElevenLabs payload | job={} record={} coach={} voice={} lang={} iso={} chars={} preview=\"{}\"",
                    job_code,
                    record_id,
                    job.payload.get("coach_id"),
                    voice_id,
                    language,
                    language_iso,
                    len(text),
                    snippet,
                )
                logger.info(
                    "[STUB] ElevenLabs params | voice_id={} model_id={} language_code={} stability={}",
                    voice_id,
                    self.MODEL_ID,
                    language_iso,
                    self.STABILITY,
                )
                logger.info(
                    "[STUB] ElevenLabs body | text={} metadata={}",
                    text,
                    json.dumps(
                        {
                            "record_id": record_id,
                            "coach_id": job.payload.get("coach_id"),
                            "subcategory_id": job.payload.get("subcategory_id"),
                            "category_id": job.payload.get("category_id"),
                            "language": language,
                            "gender": gender,
                        },
                        ensure_ascii=False,
                    ),
                )
                continue
            response = self._synthesize(voice_id, text, language_iso)
            audio_bytes, timing_payload = self._extract_outputs(response)
            duration = self._determine_duration(response, timing_payload, audio_bytes)
            if duration > self.MAX_AUDIO_DURATION_SEC:
                logger.warning(
                    "Audio duration exceeds limit",
                    job=job_code,
                    duration_sec=duration,
                    max_sec=self.MAX_AUDIO_DURATION_SEC,
                    gender=gender,
                    language=language,
                )
                raise RetryableStepError(
                    f"ElevenLabs returned audio longer than {self.MAX_AUDIO_DURATION_SEC}s"
                )

            filename_base = self._build_filename_base(
                job.payload, gender, language_iso
            )
            mp3_path = self.EXPORT_DIR / f"{filename_base}.mp3"
            json_path = self.EXPORT_DIR / f"{filename_base}.json"
            self._save_file_with_retry(
                mp3_path,
                "audio",
                lambda: mp3_path.write_bytes(audio_bytes),
            )
            self._save_file_with_retry(
                json_path,
                "timing",
                lambda: json_path.write_text(
                    json.dumps(timing_payload, ensure_ascii=False, sort_keys=True, indent=2),
                    encoding="utf-8",
                ),
            )

            self._append_duration(record_id, mp3_path.name, duration)
            self._mark_ready_voice(record_id, gender, language)
            self._maybe_preview(audio_bytes)

            completed += 1
            logger.info(
                "[BUSINESS] Audio generated | job={} gender={} lang={} duration={:.2f}s",
                job_code,
                gender,
                language,
                duration,
            )

        logger.info(
            "[PROG] Audio batch done | job={} done={}/{}",
            job_code,
            completed,
            total_segments,
        )

    # ----------------------------------------------------------------- helpers
    def _select_languages(self) -> List[LanguagePlan]:
        plans = [
            LanguagePlan(code=code, iso=code.lower())
            for code in (
                lang.strip().upper()
                for lang in self.config.languages
                if lang.strip()
            )
        ]
        if not plans:
            raise FatalStepError("Config.languages must include at least one ISO code")
        return plans

    def _fetch_categories(self) -> List[Dict[str, Any]]:
        try:
            response = (
                self.supabase.table("categories")
                .select("id, name, position")
                .order("position")
                .execute()
            )
        except Exception as exc:  # pragma: no cover - network
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
        except Exception as exc:  # pragma: no cover - network
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
                .select('id, coach, prompt, "voice_EL_ID"')
                .in_("coach", desired)
                .execute()
            )
        except Exception as exc:  # pragma: no cover - network
            raise FatalStepError(f"Failed to load coaches: {exc}") from exc

        coaches = getattr(response, "data", []) or []
        missing = sorted(set(desired).difference({row.get("coach") for row in coaches}))
        if missing:
            logger.warning("Some configured coaches not found", missing=missing)

        prepared: List[Dict[str, Any]] = []
        for coach in coaches:
            voice_id = coach.get("voice_EL_ID") or coach.get("voice_el_id")
            if not voice_id:
                raise FatalStepError(
                    f"Coach {coach.get('coach') or coach.get('id')} missing voice_EL_ID"
                )
            coach["voice_id"] = voice_id
            prepared.append(coach)
        return prepared

    def _fetch_affirmations(
        self, subcategory_id: Any, coach_id: Any
    ) -> List[Dict[str, Any]]:
        logger.info(
            "[SUPABASE] request | table=affirmations_new subcategory_id={} coach_id={}",
            subcategory_id,
            coach_id,
        )
        try:
            response = (
                self.supabase.table("affirmations_new")
                .select("id, position, script, duration, ready_voice")
                .eq("subcategory_id", subcategory_id)
                .eq("coach_id", coach_id)
                .order("position")
                .execute()
            )
        except Exception as exc:  # pragma: no cover - network
            raise RetryableStepError(
                f"Failed to load affirmations for subcategory {subcategory_id}: {exc}"
            ) from exc
        records = getattr(response, "data", []) or []
        try:
            logger.info(
                "[SUPABASE] response payload | table=affirmations_new subcategory_id={} coach_id={} data={}",
                subcategory_id,
                coach_id,
                json.dumps(records, ensure_ascii=False)[:1000],
            )
        except Exception:
            logger.info(
                "[SUPABASE] response payload | table=affirmations_new subcategory_id={} coach_id={} data=<unserializable>",
                subcategory_id,
                coach_id,
            )
        logger.info(
            "[SUPABASE] response | table=affirmations_new subcategory_id={} coach_id={} rows={}",
            subcategory_id,
            coach_id,
            len(records),
        )
        return records

    def _parse_script(self, raw: Any) -> Dict[str, Dict[str, Dict[str, str]]]:
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise RetryableStepError(f"Invalid script JSON: {exc}") from exc
        if not isinstance(raw, dict):
            return {}
        return raw

    def _synthesize(self, voice_id: str, text: str, language_code: str) -> Dict[str, Any]:
        converter = getattr(self.client.text_to_speech, "convert_with_timestamps", None)
        if converter is None:
            raise FatalStepError(
                "Installed elevenlabs SDK does not expose convert_with_timestamps"
            )
        request_meta = {
            "event": "elevenlabs_request",
            "timestamp": datetime.utcnow().isoformat(),
            "voice_id": voice_id,
            "model_id": self.MODEL_ID,
            "language_code": language_code,
            "stability": self.STABILITY,
            "text_length": len(text or ""),
        }
        self._log_llm_raw(request_meta)
        try:
            response = converter(
                voice_id=voice_id,
                model_id=self.MODEL_ID,
                text=text,
                language_code=language_code,
                voice_settings={"stability": self.STABILITY},
            )
        except Exception as exc:  # pragma: no cover - SDK errors
            raise RetryableStepError(f"ElevenLabs request failed: {exc}") from exc

        response_dict = self._to_dict(response)
        self._log_llm_raw(
            {
                "event": "elevenlabs_response",
                "timestamp": datetime.utcnow().isoformat(),
                "voice_id": voice_id,
                "model_id": self.MODEL_ID,
                "language_code": language_code,
                "stability": self.STABILITY,
                "response": self._redact_audio_payload(response_dict),
            }
        )
        return response_dict

    def _extract_outputs(self, response: Dict[str, Any]) -> Tuple[bytes, Dict[str, Any]]:
        audio_base64 = response.get("audio_base64") or response.get("audio_base_64")
        if not audio_base64:
            raise RetryableStepError("ElevenLabs response missing audio_base64")
        try:
            audio_bytes = base64.b64decode(audio_base64)
        except Exception as exc:  # pragma: no cover - decode guard
            raise RetryableStepError(f"Invalid audio payload: {exc}") from exc

        timing_payload = response.get("alignment") or response.get("timestamps")
        timing_dict = self._to_dict(timing_payload)
        if not timing_dict:
            timing_dict = {"timestamps": timing_payload or {}}
        return audio_bytes, timing_dict

    def _build_filename_base(
        self,
        job_payload: Dict[str, Any],
        gender: str,
        language_iso: str,
    ) -> str:
        cat_pos = int(job_payload.get("category_position") or 0)
        sub_pos = int(job_payload.get("subcategory_position") or 0)
        coach_pos = int(job_payload.get("coach_id") or 0)
        aff_pos = int(job_payload.get("record_position") or 0)
        gender_code = "w" if gender == "female" else "m"
        return f"{cat_pos}_{sub_pos}_{coach_pos}_{aff_pos}_{gender_code}_{language_iso}"

    def _determine_duration(
        self,
        response: Dict[str, Any],
        timing_payload: Dict[str, Any],
        audio_bytes: bytes,
    ) -> float:
        audio_duration = self._duration_from_audio(audio_bytes)
        if audio_duration is not None:
            return audio_duration
        return self._extract_duration_seconds(response, timing_payload, audio_bytes)

    def _duration_from_audio(self, audio_bytes: bytes) -> Optional[float]:
        try:
            audio_file = MutagenFile(io.BytesIO(audio_bytes))
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("Failed to parse audio for duration", error=str(exc))
            return None
        if not audio_file or not getattr(audio_file, "info", None):
            return None
        length = getattr(audio_file.info, "length", None)
        if not length:
            return None
        return float(round(length, 3))

    def _extract_duration_seconds(
        self,
        response: Dict[str, Any],
        timing_payload: Dict[str, Any],
        audio_bytes: bytes,
    ) -> float:
        candidates = [
            response.get("audio_duration_seconds"),
            response.get("duration"),
            timing_payload.get("audio_duration_seconds")
            if isinstance(timing_payload, dict)
            else None,
        ]
        for candidate in candidates:
            value = self._to_float(candidate)
            if value is not None:
                return round(value, 3)

        alignment = timing_payload if isinstance(timing_payload, dict) else {}
        char_end_times = alignment.get("character_end_times_seconds")
        if isinstance(char_end_times, list) and char_end_times:
            value = self._to_float(char_end_times[-1])
            if value is not None:
                return round(value, 3)

        value = self._extract_from_alignment(alignment)
        if value is not None:
            return round(value, 3)

        approx = max(len(audio_bytes) / 48000.0, 0.1)
        return round(approx, 3)

    def _extract_from_alignment(self, alignment: Dict[str, Any]) -> Optional[float]:
        sequences = []
        for key in ("words", "segments", "characters", "items", "timestamps"):
            seq = alignment.get(key)
            if isinstance(seq, list) and seq:
                sequences.append(seq)
        for seq in sequences:
            for item in reversed(seq):
                if isinstance(item, dict):
                    value = self._to_float(
                        item.get("end")
                        or item.get("end_time")
                        or item.get("time_end")
                        or item.get("timestamp")
                    )
                else:
                    value = None
                if value is not None:
                    return value
        if isinstance(alignment.get("duration"), (int, float)):
            return float(alignment.get("duration"))
        return None

    def _append_duration(self, record_id: Any, filename: str, duration: float) -> None:
        with self._duration_lock:
            current = dict(self._duration_cache.get(record_id, {}))
            current[filename] = float(round(duration, 3))
            self._duration_cache[record_id] = current
        self._persist_duration(record_id, current)

    def _reset_duration_if_needed(self, record_id: Any) -> None:
        with self._duration_lock:
            if record_id in self._duration_reset:
                return
            self._duration_reset.add(record_id)
            self._duration_cache[record_id] = {}
        self._persist_duration(record_id, {})

    def _persist_duration(self, record_id: Any, payload: Dict[str, float]) -> None:
        normalized = json.loads(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        try:
            (
                self.supabase.table("affirmations_new")
                .update({"duration": normalized})
                .eq("id", record_id)
                .execute()
            )
        except Exception as exc:  # pragma: no cover - network
            raise RetryableStepError(
                f"Failed to update duration for {record_id}: {exc}"
            ) from exc

    def _seed_duration_cache(self, record: Dict[str, Any]) -> None:
        record_id = record.get("id")
        if record_id in self._duration_cache:
            return
        raw = record.get("duration")
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except json.JSONDecodeError:
                raw = {}
        if not isinstance(raw, dict):
            raw = {}
        self._duration_cache[record_id] = {
            str(key): float(value)
            for key, value in raw.items()
            if isinstance(value, (int, float))
        }

    def _seed_ready_voice_cache(self, record: Dict[str, Any]) -> None:
        record_id = record.get("id")
        if record_id is None:
            return
        with self._ready_voice_lock:
            if record_id in self._ready_voice_cache:
                return
            state = self._parse_ready_map(record.get("ready_voice"))
            self._ready_voice_cache[record_id] = state

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

    def _maybe_preview(self, audio_bytes: bytes) -> None:
        if not self.preview_audio:
            return
        try:  # pragma: no cover - interactive branch
            play(audio_bytes)
        except Exception as exc:
            logger.warning("Audio preview failed", error=str(exc))

    def _is_voice_ready(self, record_id: Any, gender: str, language: str) -> bool:
        with self._ready_voice_lock:
            state = self._ready_voice_cache.get(record_id)
            if not state:
                return False
            return language.upper() in state.get(gender, set())

    def _mark_ready_voice(self, record_id: Any, gender: str, language: str) -> None:
        lang = language.upper()
        with self._ready_voice_lock:
            state = self._ready_voice_cache.setdefault(record_id, self._empty_ready_state())
            if lang in state[gender]:
                return
            state[gender].add(lang)
            payload = self._format_ready_map(state)
        self._persist_ready_field(record_id, payload, "ready_voice")

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
        except Exception as exc:  # pragma: no cover - network
            raise RetryableStepError(
                f"Failed to update {field_name} for {record_id}: {exc}"
            ) from exc

    @staticmethod
    def _normalize_lang_entry(entry: Any) -> Dict[str, Any]:
        if isinstance(entry, dict):
            return entry
        if isinstance(entry, str):
            return {"script": entry}
        return {}

    def _save_file_with_retry(
        self,
        path: Path,
        description: str,
        write_callable: Callable[[], None],
        attempts: int = 3,
        delay_sec: float = 5.0,
    ) -> None:
        for attempt in range(1, attempts + 1):
            try:
                write_callable()
                logger.info(
                    "[IO] File saved | type={} path={} attempt={}/{}",
                    description,
                    str(path),
                    attempt,
                    attempts,
                )
                return
            except Exception as exc:
                logger.warning(
                    "[IO] File save failed | type={} path={} attempt={}/{} error={}",
                    description,
                    str(path),
                    attempt,
                    attempts,
                    exc,
                )
                if attempt == attempts:
                    raise RetryableStepError(
                        f"Failed to save {description} file at {path}: {exc}"
                    ) from exc
                time.sleep(delay_sec)

    def _log_llm_raw(self, payload: Dict[str, Any]) -> None:
        try:
            LLMClient._init_raw_log()
            LLMClient._log_raw(payload)
        except Exception:
            pass

    @staticmethod
    def _redact_audio_payload(data: Any) -> Any:
        if isinstance(data, dict):
            sanitized: Dict[str, Any] = {}
            for key, value in data.items():
                if key in {"audio_base64", "audio_base_64", "audio"}:
                    sanitized[key] = "<<omitted>>"
                else:
                    sanitized[key] = Step6GenerateAudio._redact_audio_payload(value)
            return sanitized
        if isinstance(data, list):
            return [Step6GenerateAudio._redact_audio_payload(item) for item in data]
        return data

    @staticmethod
    def _to_float(value: Any) -> Optional[float]:
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _to_dict(obj: Any) -> Dict[str, Any]:
        if isinstance(obj, dict):
            return obj
        if hasattr(obj, "model_dump"):
            return obj.model_dump()  # type: ignore[attr-defined]
        if hasattr(obj, "dict"):
            return obj.dict()  # type: ignore[call-arg]
        return {}


__all__ = ["Step6GenerateAudio"]
