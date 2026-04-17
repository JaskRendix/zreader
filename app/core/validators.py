from __future__ import annotations

from typing import Any, AsyncIterator

from pydantic import RootModel, ValidationError


class NDJSONRecord(RootModel[dict[str, Any]]):
    """
    Default NDJSON object schema.
    Accepts arbitrary JSON objects by default.
    Users can override this with their own Pydantic models.
    """


class NDJSONValidator:
    """
    Validates NDJSON objects using a Pydantic model.
    """

    def __init__(self, model: type[RootModel] = NDJSONRecord) -> None:
        self.model = model

    def validate(self, raw_line: str) -> dict[str, Any] | None:
        """
        Validate a single NDJSON line.
        Returns the parsed dict or None if validation fails.
        """
        if not raw_line.strip():
            return None

        try:
            model = self.model.model_validate_json(raw_line)
            return model.root  # extract underlying dict
        except ValidationError:
            return None

    async def validate_stream(
        self,
        lines: AsyncIterator[str],
    ) -> AsyncIterator[dict[str, Any]]:
        """
        Validate an async stream of NDJSON lines.
        Yields only valid dict objects.
        """
        async for line in lines:
            obj = self.validate(line)
            if obj is not None:
                yield obj
