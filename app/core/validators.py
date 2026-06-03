from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, AsyncIterator, TypeVar

from pydantic import RootModel, ValidationError

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=RootModel[Any])


class NDJSONRecord(RootModel[dict[str, Any]]):
    """
    Default NDJSON record schema.
    Accepts any JSON object. Override with a stricter model as needed.
    """


@dataclass
class ValidationStats:
    """
    Per-stream counters for NDJSON validation.

    valid:          number of successfully parsed and schema-valid lines
    invalid_json:   lines that fail JSON syntax parsing
    schema_errors:  lines that are valid JSON but fail schema validation
    empty:          blank or whitespace-only lines
    """

    valid: int = 0
    invalid_json: int = 0
    schema_errors: int = 0
    empty: int = 0

    @property
    def total_rejected(self) -> int:
        return self.invalid_json + self.schema_errors + self.empty


class ValidationError_(Exception):
    """Raised in strict mode when a line cannot be parsed or validated."""


class NDJSONValidator:
    """
    High-throughput NDJSON validator using a Pydantic RootModel.

    This implementation performs **single-pass** validation using
    `model_validate_json()`, which handles both JSON parsing and schema
    validation inside Pydantic's Rust engine.

    Features:
    - Single-pass parsing (no json.loads double-work)
    - Optional strict mode (fail-fast)
    - Per-stream stats isolation for concurrency safety
    - Unified validation logic for both single-line and streaming use
    """

    def __init__(
        self,
        model: type[RootModel[Any]] = NDJSONRecord,
        strict: bool = False,
    ) -> None:
        self.model = model
        self.strict = strict
        self._fallback_stats = ValidationStats()

    @property
    def stats(self) -> ValidationStats:
        """
        Backwards-compatible accessor for stats.

        For streaming validation, a dedicated stats object is passed
        explicitly to avoid cross-request contamination.
        """
        return self._fallback_stats

    def validate(
        self,
        raw_line: str,
        stats_accumulator: ValidationStats | None = None,
    ) -> dict[str, Any] | None:
        """
        Validate a single NDJSON line.

        - Blank lines increment `empty` and return None.
        - Malformed JSON increments `invalid_json`.
        - Schema-invalid JSON increments `schema_errors`.
        - In strict mode, malformed or schema-invalid lines raise ValidationError_.
        - On success, returns the parsed dict.
        """
        stats = (
            stats_accumulator if stats_accumulator is not None else self._fallback_stats
        )

        if not raw_line or not raw_line.strip():
            stats.empty += 1
            return None

        try:
            record = self.model.model_validate_json(raw_line)
            stats.valid += 1
            return record.root  # type: ignore[attr-defined]

        except ValidationError as exc:
            # Distinguish JSON syntax errors from schema errors
            is_json_syntax_error = any(
                "json_invalid" in err.get("type", "") for err in exc.errors()
            )

            if is_json_syntax_error:
                stats.invalid_json += 1
                if self.strict:
                    raise ValidationError_(f"Invalid JSON: {exc}") from exc
                logger.warning("Skipping malformed JSON line: %.120s", raw_line)
            else:
                stats.schema_errors += 1
                if self.strict:
                    raise ValidationError_(f"Schema error: {exc}") from exc
                logger.warning("Skipping line failing schema validation: %s", exc)

            return None

    async def validate_stream(
        self,
        lines: AsyncIterator[str],
        stats: ValidationStats | None = None,
    ) -> AsyncIterator[dict[str, Any]]:
        """
        Validate an async NDJSON line stream.

        - Uses a per-stream ValidationStats instance for concurrency safety.
        - Delegates all validation to `validate()` to keep logic DRY.
        - Yields only valid dicts; invalid lines are logged and counted.
        """
        stream_stats = stats if stats is not None else ValidationStats()
        self._fallback_stats = stream_stats  # backwards compatibility

        async for line in lines:
            validated = self.validate(line, stats_accumulator=stream_stats)
            if validated is not None:
                yield validated
