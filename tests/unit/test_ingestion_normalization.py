from __future__ import annotations

from decimal import Decimal

import pytest

from domain import PendingCode
from ingestion import (
    NormalizationError,
    normalize_hours_hhmm,
    normalize_money_brl,
    normalize_quantity,
    validate_competence,
)


def test_validate_competence_accepts_mm_yyyy():
    assert validate_competence("03/2024") == "03/2024"


def test_validate_competence_rejects_invalid_value():
    with pytest.raises(NormalizationError, match="Competencia invalida"):
        validate_competence("2024-03")


def test_normalize_money_brl_accepts_numeric_and_brazilian_text():
    assert normalize_money_brl(123.4) == Decimal("123.40")
    assert normalize_money_brl("1.234,56") == Decimal("1234.56")


def test_normalize_money_brl_rejects_ambiguous_text():
    with pytest.raises(NormalizationError) as exc_info:
        normalize_money_brl("1.234")

    assert exc_info.value.code == PendingCode.INVALID_VALUE


def test_normalize_hours_hhmm_accepts_valid_string():
    normalized = normalize_hours_hhmm("02:16")

    assert normalized.text == "02:16"
    assert normalized.total_minutes == 136


def test_normalize_hours_hhmm_rejects_invalid_minutes():
    with pytest.raises(NormalizationError) as exc_info:
        normalize_hours_hhmm("01:75")

    assert exc_info.value.code == PendingCode.INVALID_HOUR


def test_normalize_quantity_accepts_decimal_text():
    assert normalize_quantity("1,5") == Decimal("1.5")
