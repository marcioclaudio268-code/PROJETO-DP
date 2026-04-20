import pytest

from serialization.layout import (
    LAYOUT_43_FIELDS,
    LAYOUT_43_TOTAL_WIDTH,
    layout_43_field_names,
    layout_43_widths,
)
from validation.layout import validate_layout_43_line


def test_layout_43_spec_has_seven_fields_and_correct_widths():
    assert len(LAYOUT_43_FIELDS) == 7
    assert LAYOUT_43_TOTAL_WIDTH == 43
    assert layout_43_widths() == (1, 11, 6, 4, 2, 9, 10)
    assert layout_43_field_names() == (
        "campo_01",
        "campo_02",
        "campo_03",
        "campo_04",
        "campo_05",
        "campo_06",
        "campo_07",
    )


def test_layout_43_validator_accepts_exact_width():
    validate_layout_43_line("x" * 43)


def test_layout_43_validator_rejects_wrong_width():
    with pytest.raises(ValueError, match="exactly 43 characters"):
        validate_layout_43_line("x" * 42)
