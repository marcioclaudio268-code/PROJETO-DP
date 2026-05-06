"""Structural validation helpers for the 43-character TXT layout."""

from __future__ import annotations

from serialization.layout import LAYOUT_43_FIELDS, LAYOUT_43_TOTAL_WIDTH


def validate_layout_43_line(line: str) -> None:
    """Validate that a TXT line has exactly 43 characters.
    """

    if not isinstance(line, str):
        raise TypeError("line must be a str")

    actual_width = len(line)
    if actual_width != LAYOUT_43_TOTAL_WIDTH:
        raise ValueError(
            f"layout_43 line must have exactly {LAYOUT_43_TOTAL_WIDTH} characters; got {actual_width}"
        )


def split_layout_43_line(line: str) -> dict[str, str]:
    """Split one 43-character line into named fixed-width fields."""

    validate_layout_43_line(line)

    fields: dict[str, str] = {}
    offset = 0
    for field in LAYOUT_43_FIELDS:
        fields[field.name] = line[offset : offset + field.width]
        offset += field.width
    return fields


def validate_layout_43_structural_fields(line: str) -> dict[str, str]:
    """Validate width plus the fixed structural contract of the layout."""

    fields = split_layout_43_line(line)

    if fields["tipo_registro"] != "1":
        raise ValueError(
            f"layout_43 field 'tipo_registro' must be '1'; got '{fields['tipo_registro']}'"
        )

    for field_name in (
        "matricula_dominio",
        "competencia",
        "rubrica_saida",
        "codigo_processo",
        "valor_ou_referencia",
        "codigo_empresa",
    ):
        if not fields[field_name].isdigit():
            raise ValueError(
                f"layout_43 field '{field_name}' must contain only digits; got '{fields[field_name]}'"
            )

    return fields
