"""Structural validation helpers for the 43-character TXT layout."""

from __future__ import annotations

from serialization.layout import LAYOUT_43_TOTAL_WIDTH


def validate_layout_43_line(line: str) -> None:
    """Validate that a TXT line has exactly 43 characters.

    The serializer is not implemented yet, so this function only checks width.
    """

    if not isinstance(line, str):
        raise TypeError("line must be a str")

    actual_width = len(line)
    if actual_width != LAYOUT_43_TOTAL_WIDTH:
        raise ValueError(
            f"layout_43 line must have exactly {LAYOUT_43_TOTAL_WIDTH} characters; got {actual_width}"
        )
