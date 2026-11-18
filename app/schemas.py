from __future__ import annotations

from typing import Dict

from pydantic import BaseModel, Field, field_validator


class ErasureRequest(BaseModel):
    user_id: str = Field(min_length=1, max_length=512)


class ReconciliationRequest(BaseModel):
    """A report checklist: the UTC day and the click count expected per page."""

    date: str = Field(min_length=10, max_length=10)
    expected_counts: Dict[str, int] = Field(alias="pages", min_length=1)

    model_config = {"populate_by_name": True}

    @field_validator("date")
    @classmethod
    def _check_date_shape(cls, value: str) -> str:
        from datetime import datetime

        try:
            datetime.strptime(value, "%Y-%m-%d")
        except ValueError as exc:
            raise ValueError("date must be a valid UTC calendar day YYYY-MM-DD") from exc
        return value

    @field_validator("expected_counts")
    @classmethod
    def _check_counts(cls, value: Dict[str, int]) -> Dict[str, int]:
        if any(not page.strip() for page in value):
            raise ValueError("page names must be non-empty strings")
        if any(count < 0 for count in value.values()):
            raise ValueError("expected click counts must be non-negative integers")
        return value
