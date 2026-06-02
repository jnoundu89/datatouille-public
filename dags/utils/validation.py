"""Data validation helpers and Pydantic schemas for the Medallion Architecture."""

import logging
from typing import Any

from pydantic import BaseModel, Field, ValidationError

logger = logging.getLogger(__name__)


def validate_records(
    records: list[dict[str, Any]],
    model: type[BaseModel],
    min_pass_rate: float = 0.90,
) -> list[dict[str, Any]]:
    """Validate a list of records against a Pydantic model.

    If the proportion of valid records is below min_pass_rate, raises ValueError to block pipeline progression.
    """
    valid_records = []
    errors = 0

    for r in records:
        try:
            # Instantiate model to perform schema/type checks
            validated = model(**r)
            valid_records.append(validated.model_dump())
        except ValidationError as e:
            errors += 1
            if errors <= 5:
                logger.warning("Validation error on record: %s\nError details: %s", r, e)

    total = len(records)
    if total == 0:
        raise ValueError("Cannot validate empty records list")

    pass_rate = len(valid_records) / total
    logger.info(
        "Validation completed: %d valid, %d errors out of %d total (Pass rate: %.2f%%)",
        len(valid_records),
        errors,
        total,
        pass_rate * 100,
    )

    if pass_rate < min_pass_rate:
        raise ValueError(
            f"Schema quality gate failed: pass rate {pass_rate:.2f} is below threshold {min_pass_rate:.2f}"
        )

    return valid_records


class BookRecord(BaseModel):
    upc: str = Field(..., min_length=2)
    title: str
    category: str
    star_rating: int = Field(..., ge=1, le=5)
    price: float = Field(..., ge=0.0)
    price_incl_tax: float = Field(..., ge=0.0)
    tax: float = Field(..., ge=0.0)
    availability: str
    stock_count: int = Field(..., ge=0)
    description: str | None = None
    image_url: str
    product_url: str
    extraction_date: str
    extracted_at: str
