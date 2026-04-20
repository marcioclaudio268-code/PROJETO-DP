"""Fixed-width layout metadata for the Dominio TXT import.

This module only holds the structural contract for the 43-character line.
It does not implement the full serializer.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class LayoutFieldSpec:
    """Structural metadata for one fixed-width field."""

    name: str
    width: int
    type_hint: str = "ABERTO"
    padding: str = "ABERTO"


LAYOUT_43_FIELDS: tuple[LayoutFieldSpec, ...] = (
    LayoutFieldSpec(name="campo_01", width=1),
    LayoutFieldSpec(name="campo_02", width=11),
    LayoutFieldSpec(name="campo_03", width=6),
    LayoutFieldSpec(name="campo_04", width=4),
    LayoutFieldSpec(name="campo_05", width=2),
    LayoutFieldSpec(name="campo_06", width=9),
    LayoutFieldSpec(name="campo_07", width=10),
)

LAYOUT_43_TOTAL_WIDTH: int = sum(field.width for field in LAYOUT_43_FIELDS)


def layout_43_widths() -> tuple[int, ...]:
    """Return the tuple of widths that define the layout."""

    return tuple(field.width for field in LAYOUT_43_FIELDS)


def layout_43_field_names() -> tuple[str, ...]:
    """Return the ordered field names used by the current structural spec."""

    return tuple(field.name for field in LAYOUT_43_FIELDS)
