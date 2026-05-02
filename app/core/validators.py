from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
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
    """Running counters for a validate_stream pass."""

    valid: int = 0
    invalid_json: int = 0
    schema_errors: int = 0
    empty: int = 0

    @property
    def total_rejected(self) -> int:
        return self.invalid_json + self.schema_errors + self.empty


class ValidationError_(Exception):
    """Raised by validate() in strict mode when a line cannot be parsed."""


class NDJSONValidator:
    """
    Validates NDJSON lines using a Pydantic RootModel.

    Parameters
    ----------
    model:
        A RootModel subclass used to validate each line.
        Defaults to NDJSONRecord (accepts any JSON object).
    strict:
        If True, validate() raises on malformed JSON or schema violations
        instead of returning None. Useful for fail-fast pipelines.
    """

    def __init__(
        self,
        model: type[RootModel[Any]] = NDJSONRecord,
        strict: bool = False,
    ) -> None:
        self.model = model
        self.strict = strict

    def validate(self, raw_line: str) -> dict[str, Any] | None:
        """
        Validate a single NDJSON line.

        Returns the parsed dict on success.
        Returns None for blank lines (expected, not an error).
        Returns None for invalid lines when strict=False.
        Raises ValidationError_ for invalid lines when strict=True,
        distinguishing json.JSONDecodeError (malformed) from
        pydantic.ValidationError (schema mismatch).
        """
        if not raw_line.strip():
            return None

        # First check: is it valid JSON at all?
        try:
            json.loads(raw_line)
        except json.JSONDecodeError as exc:
            if self.strict:
                raise ValidationError_(f"Invalid JSON: {exc}") from exc
            return None

        # Second check: does it match the schema?
        try:
            record = self.model.model_validate_json(raw_line)
            return record.root  # type: ignore[attr-defined]
        except ValidationError as exc:
            if self.strict:
                raise ValidationError_(f"Schema error: {exc}") from exc
            return None

    async def validate_stream(
        self,
        lines: AsyncIterator[str],
    ) -> AsyncIterator[dict[str, Any]]:
        """
        Validate an async stream of raw NDJSON lines.

        Yields only valid dicts. Invalid and empty lines are counted and
        logged; access .stats after (or during) iteration for observability.
        """
        self.stats = ValidationStats()

        async for line in lines:
            if not line.strip():
                self.stats.empty += 1
                continue

            # Separate JSON parse errors from schema errors for clearer logs.
            try:
                json.loads(line)
            except json.JSONDecodeError:
                self.stats.invalid_json += 1
                logger.warning("Skipping malformed JSON line: %.120s", line)
                continue

            try:
                record = self.model.model_validate_json(line)
                self.stats.valid += 1
                yield record.root  # type: ignore[attr-defined]
            except ValidationError as exc:
                self.stats.schema_errors += 1
                logger.warning("Skipping line failing schema validation: %s", exc)
