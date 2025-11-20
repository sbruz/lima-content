from __future__ import annotations

import os
from functools import lru_cache
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Type, Union
import copy
from datetime import datetime

from dotenv import load_dotenv
from openai import OpenAI
from openai.types.responses import Response, ResponseFormatTextJSONSchemaConfig
from pydantic import BaseModel

from src.runtime.logging import logger

load_dotenv()


def _ensure_openai_client() -> OpenAI:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not configured")
    return OpenAI(api_key=api_key)


@lru_cache()
def _client() -> OpenAI:
    return _ensure_openai_client()


class LLMClient:
    """Wrapper around the official OpenAI Responses API."""
    RAW_LOG_PATH = Path("logs/llm_raw.log")
    _raw_log_initialized = False

    def __init__(
        self,
        *,
        api_key: Optional[str] = None,
        request_timeout: Optional[float] = None,
    ) -> None:
        if api_key:
            self._client = OpenAI(api_key=api_key)
        else:
            self._client = _client()
        env_timeout = os.getenv("OPENAI_REQUEST_TIMEOUT")
        self._request_timeout = (
            request_timeout
            if request_timeout is not None
            else float(env_timeout) if env_timeout else 600.0
        )
        self._init_raw_log()

    def chat(
        self,
        messages: List[Dict[str, str]],
        *,
        model: str,
        temperature: float = 0.7,
        max_tokens: Optional[int] = None,
        response_schema: Optional[Union[Dict[str, Any], Type[BaseModel]]] = None,
    ) -> Dict[str, Any]:
        logger.info(
            "OpenAI request payload",
            model=model,
            messages=messages,
            response_schema=self._describe_schema(response_schema),
        )

        self._log_raw(
            {
                "event": "request",
                "timestamp": datetime.utcnow().isoformat(),
                "model": model,
                "messages": messages,
                "response_schema": self._describe_schema(response_schema),
            }
        )

        try:
            if response_schema is not None:
                resp_kwargs: Dict[str, Any] = {
                    "model": model,
                    "input": messages,
                }
                text_format = self._schema_to_text_format(response_schema)
                if text_format is not None:
                    resp_kwargs["text_format"] = text_format
                if max_tokens is not None:
                    resp_kwargs["max_output_tokens"] = max_tokens
                resp_kwargs["timeout"] = self._request_timeout
                response = self._client.responses.parse(**resp_kwargs)
                self._log_raw(
                    {
                        "event": "response",
                        "timestamp": datetime.utcnow().isoformat(),
                        "model": model,
                        "raw": response.model_dump(),
                    }
                )
                parsed = self._normalize_parsed_response(response)
                return parsed
            else:
                create_kwargs: Dict[str, Any] = {
                    "model": model,
                    "input": self._convert_messages(messages),
                }
                if max_tokens is not None:
                    create_kwargs["max_output_tokens"] = max_tokens
                create_kwargs["timeout"] = self._request_timeout
                response = self._client.responses.create(**create_kwargs)
                self._log_raw(
                    {
                        "event": "response",
                        "timestamp": datetime.utcnow().isoformat(),
                        "model": model,
                        "raw": response.model_dump(),
                    }
                )
                normalized = self._normalize_response(response)
                return normalized
        except Exception as exc:  # pragma: no cover - network failure path
            logger.error(
                "OpenAI request failed",
                error=str(exc),
                error_type=exc.__class__.__name__,
                error_repr=repr(exc),
            )
            self._log_raw(
                {
                    "event": "error",
                    "timestamp": datetime.utcnow().isoformat(),
                    "model": model,
                    "error": str(exc),
                    "error_type": exc.__class__.__name__,
                }
            )
            raise

    def close(self) -> None:
        # official client does not expose close()
        pass

    @classmethod
    def _init_raw_log(cls) -> None:
        if cls._raw_log_initialized:
            return
        path = cls.RAW_LOG_PATH
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            path.unlink()
        path.touch()
        cls._raw_log_initialized = True

    @classmethod
    def _log_raw(cls, payload: Dict[str, Any]) -> None:
        try:
            with cls.RAW_LOG_PATH.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(payload, ensure_ascii=False) + "\n")
        except Exception:  # pragma: no cover - logging must not break main flow
            pass

    def _convert_messages(
        self, messages: Iterable[Dict[str, str]]
    ) -> List[Dict[str, Any]]:
        converted = []
        for message in messages:
            converted.append(
                {
                    "role": message["role"],
                    "content": [
                        {
                            "type": "input_text",
                            "text": message.get("content", ""),
                        }
                    ],
                }
            )
        return converted

    def _normalize_response(self, response: Response) -> Dict[str, Any]:
        choices = []
        for item in response.output:
            if item.type != "message":
                continue
            text_blocks = [
                block.text
                for block in item.content
                if hasattr(block, "text") and block.type == "output_text"
            ]
            choices.append(
                {
                    "message": {
                        "role": item.role,
                        "content": "".join(text_blocks),
                    }
                }
            )

        return {
            "choices": choices,
            "usage": getattr(response, "usage", None),
            "id": response.id,
        }

    def _normalize_parsed_response(self, response: Response) -> Dict[str, Any]:
        parsed = getattr(response, "output_parsed", None)
        if parsed is None:
            return self._normalize_response(response)

        if isinstance(parsed, BaseModel):
            parsed_dict = parsed.model_dump()
        elif isinstance(parsed, (dict, list)):
            parsed_dict = parsed
        else:
            parsed_dict = parsed

        return {
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "content": json.dumps(parsed_dict, ensure_ascii=False)
                    }
                }
            ],
            "usage": getattr(response, "usage", None),
            "id": response.id,
        }

    @staticmethod
    def _describe_schema(schema: Any) -> Any:
        if schema is None:
            return None
        if isinstance(schema, ResponseFormatTextJSONSchemaConfig):
            return schema.model_dump()
        if isinstance(schema, BaseModel):
            return schema.__class__.__name__
        if isinstance(schema, type) and issubclass(schema, BaseModel):
            return schema.__name__
        if hasattr(schema, "model_json_schema"):
            try:
                return schema.model_json_schema()
            except Exception:  # pragma: no cover - defensive
                return repr(schema)
        try:
            json.dumps(schema)
            return schema
        except TypeError:
            return repr(schema)

    @staticmethod
    def _ensure_schema_defaults(schema: Any) -> Any:
        if isinstance(schema, dict):
            schema_copy = copy.deepcopy(schema)
            schema_copy.setdefault("additionalProperties", False)
            return schema_copy
        return schema

    def _schema_to_text_format(self, schema: Any) -> Any:
        if schema is None:
            return None
        if isinstance(schema, type) and issubclass(schema, BaseModel):
            return schema
        if isinstance(schema, BaseModel):
            return schema.__class__
        if isinstance(schema, ResponseFormatTextJSONSchemaConfig):
            return schema
        if isinstance(schema, dict):
            schema_type = schema.get("type")
            if schema_type == "json_schema":
                json_schema = schema.get("json_schema") or {}
                schema_body = json_schema.get("schema")
                if schema_body is None:
                    schema_body = {k: v for k, v in json_schema.items() if k != "name"}
                name = (
                    json_schema.get("name")
                    or schema.get("name")
                    or schema_body.get("title")
                    or "ResponseFormat"
                )
                return ResponseFormatTextJSONSchemaConfig(
                    type="json_schema",
                    name=name,
                    schema=self._ensure_schema_defaults(schema_body),
                )
            elif "properties" in schema or "items" in schema:
                name = schema.get("title") or "ResponseFormat"
                return ResponseFormatTextJSONSchemaConfig(
                    type="json_schema",
                    name=name,
                    schema=self._ensure_schema_defaults(schema),
                )
        raise TypeError("Unsupported response schema type for OpenAI client")

    # Alias for backward compatibility
    def _normalize_schema(self, schema: Any) -> Any:
        return self._schema_to_text_format(schema)


__all__ = ["LLMClient"]
