"""Fixed-width encoder for the 43-character TXT layout."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
import re

from .errors import SerializationEncodingError
from .layout import LAYOUT_43_FIELDS
from .models import (
    SerializableMappedMovement,
    SerializedTxtLine,
    SerializationResult,
    SerializationSkipCode,
    SerializationSkipItem,
)


def serialize_loaded_mapped_artifact(artifact) -> SerializationResult:
    serialized_lines: list[SerializedTxtLine] = []
    skipped_items: list[SerializationSkipItem] = []
    next_line_number = 1

    for movement in artifact.movements:
        skip_item = evaluate_serialization_eligibility(movement)
        if skip_item is not None:
            skipped_items.append(skip_item)
            continue

        try:
            line = encode_mapped_movement_to_txt_line(movement)
        except SerializationEncodingError as exc:
            skipped_items.append(
                SerializationSkipItem(
                    canonical_movement_id=movement.canonical_movement_id,
                    reason_code=_encoding_error_to_skip_code(exc),
                    message=str(exc),
                )
            )
            continue

        serialized_lines.append(
            SerializedTxtLine(
                canonical_movement_id=movement.canonical_movement_id,
                line_number=next_line_number,
                text=line,
            )
        )
        next_line_number += 1

    return SerializationResult(
        metadata=artifact.metadata,
        total_mapped_movements=len(artifact.movements),
        serialized_lines=tuple(serialized_lines),
        skipped_items=tuple(skipped_items),
    )


def evaluate_serialization_eligibility(
    movement: SerializableMappedMovement,
) -> SerializationSkipItem | None:
    if movement.status.value != "pronto_para_serializer":
        return SerializationSkipItem(
            canonical_movement_id=movement.canonical_movement_id,
            reason_code=SerializationSkipCode.MOVEMENT_NOT_READY,
            message="Movimento nao esta pronto para serializer e foi excluido do TXT.",
        )

    if movement.resolved_domain_registration is None:
        return SerializationSkipItem(
            canonical_movement_id=movement.canonical_movement_id,
            reason_code=SerializationSkipCode.MISSING_DOMAIN_REGISTRATION,
            message="Movimento sem matricula final resolvida nao pode ser serializado.",
        )

    if movement.output_rubric is None:
        return SerializationSkipItem(
            canonical_movement_id=movement.canonical_movement_id,
            reason_code=SerializationSkipCode.MISSING_OUTPUT_RUBRIC,
            message="Movimento sem rubrica de saida resolvida nao pode ser serializado.",
        )

    return None


def encode_mapped_movement_to_txt_line(movement: SerializableMappedMovement) -> str:
    field_values = (
        "1",
        _encode_numeric_identifier(
            movement.resolved_domain_registration,
            width=LAYOUT_43_FIELDS[1].width,
            field_name=LAYOUT_43_FIELDS[1].name,
        ),
        _encode_competence(
            movement.competence,
            width=LAYOUT_43_FIELDS[2].width,
            field_name=LAYOUT_43_FIELDS[2].name,
        ),
        _encode_numeric_identifier(
            movement.output_rubric,
            width=LAYOUT_43_FIELDS[3].width,
            field_name=LAYOUT_43_FIELDS[3].name,
        ),
        _encode_numeric_identifier(
            movement.default_process,
            width=LAYOUT_43_FIELDS[4].width,
            field_name=LAYOUT_43_FIELDS[4].name,
        ),
        _encode_value_or_reference_field(movement),
        _encode_numeric_identifier(
            movement.company_code,
            width=LAYOUT_43_FIELDS[6].width,
            field_name=LAYOUT_43_FIELDS[6].name,
        ),
    )
    line = "".join(field_values)
    from validation.layout import validate_layout_43_line

    validate_layout_43_line(line)
    return line


def render_serialized_txt(result: SerializationResult) -> str:
    if not result.serialized_lines:
        return ""
    return "\n".join(line.text for line in result.serialized_lines) + "\n"


def _encode_numeric_identifier(value: str | None, *, width: int, field_name: str) -> str:
    if value is None or value == "":
        raise SerializationEncodingError(
            "campo_numerico_invalido",
            f"Campo '{field_name}' esta ausente para o serializer.",
            field_name=field_name,
        )

    text = str(value).strip()
    if not text.isdigit():
        raise SerializationEncodingError(
            "campo_numerico_invalido",
            f"Campo '{field_name}' deve conter apenas digitos; recebido '{text}'.",
            field_name=field_name,
        )
    if len(text) > width:
        raise SerializationEncodingError(
            "campo_excede_largura",
            f"Campo '{field_name}' excede largura {width}: '{text}'.",
            field_name=field_name,
        )
    return text.zfill(width)


def _encode_competence(value: str | None, *, width: int, field_name: str) -> str:
    if value is None or value == "":
        raise SerializationEncodingError(
            "campo_numerico_invalido",
            f"Campo '{field_name}' esta ausente para o serializer.",
            field_name=field_name,
        )

    text = str(value).strip()
    if re.fullmatch(r"\d{6}", text):
        month = int(text[:2])
        year = text[2:]
        if 1 <= month <= 12:
            normalized = f"{year}{text[:2]}"
        else:
            normalized = text
    elif re.fullmatch(r"\d{2}/\d{4}", text):
        month, year = text.split("/")
        normalized = f"{year}{month}"
    else:
        raise SerializationEncodingError(
            "campo_numerico_invalido",
            f"Campo '{field_name}' possui formato invalido: '{text}'.",
            field_name=field_name,
        )

    if len(normalized) != width or not normalized.isdigit():
        raise SerializationEncodingError(
            "campo_numerico_invalido",
            f"Campo '{field_name}' possui formato invalido: '{text}'.",
            field_name=field_name,
        )
    return normalized


def _encode_value_or_reference_field(movement: SerializableMappedMovement) -> str:
    width = LAYOUT_43_FIELDS[5].width

    if movement.value_type.value == "monetario":
        if movement.amount is None:
            raise SerializationEncodingError(
                "valor_movimento_invalido",
                "Movimento monetario sem valor nao pode ser serializado.",
                field_name=LAYOUT_43_FIELDS[5].name,
            )
        return _encode_implied_decimal(
            movement.amount,
            width=width,
            field_name=LAYOUT_43_FIELDS[5].name,
        )

    if movement.value_type.value == "horas":
        if movement.hours_text is None:
            raise SerializationEncodingError(
                "valor_movimento_invalido",
                "Movimento de horas sem payload de horas nao pode ser serializado.",
                field_name=LAYOUT_43_FIELDS[5].name,
            )
        return _encode_hours_payload(movement.hours_text, width=width)

    if movement.value_type.value in {"dias", "quantidade"}:
        if movement.quantity is None:
            raise SerializationEncodingError(
                "valor_movimento_invalido",
                "Movimento de dias sem quantidade nao pode ser serializado.",
                field_name=LAYOUT_43_FIELDS[5].name,
            )
        return _encode_implied_decimal(
            movement.quantity,
            width=width,
            field_name=LAYOUT_43_FIELDS[5].name,
        )

    raise SerializationEncodingError(
        "valor_movimento_invalido",
        f"Tipo de valor nao suportado pelo serializer: {movement.value_type}.",
        field_name=LAYOUT_43_FIELDS[5].name,
    )


def _encode_hours_payload(value: str, *, width: int) -> str:
    parts = value.split(":")
    if len(parts) != 2 or not all(part.isdigit() for part in parts):
        raise SerializationEncodingError(
            "valor_movimento_invalido",
            f"Hora invalida para serializer: '{value}'.",
            field_name=LAYOUT_43_FIELDS[5].name,
        )

    hour_text, minute_text = parts
    hours = int(hour_text)
    minutes = int(minute_text)
    if minutes > 59:
        raise SerializationEncodingError(
            "valor_movimento_invalido",
            f"Hora invalida para serializer: '{value}'.",
            field_name=LAYOUT_43_FIELDS[5].name,
        )

    raw_digits = f"{hours:02d}{minutes:02d}"
    if len(raw_digits) > width:
        raise SerializationEncodingError(
            "campo_excede_largura",
            f"Campo '{LAYOUT_43_FIELDS[5].name}' excede largura {width}: '{raw_digits}'.",
            field_name=LAYOUT_43_FIELDS[5].name,
        )
    return raw_digits.zfill(width)


def _encode_implied_decimal(value: str, *, width: int, field_name: str) -> str:
    try:
        decimal_value = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise SerializationEncodingError(
            "valor_movimento_invalido",
            f"Campo '{field_name}' nao e numerico: '{value}'.",
            field_name=field_name,
        ) from exc

    scaled = decimal_value * 100
    if scaled != scaled.to_integral_value():
        raise SerializationEncodingError(
            "valor_movimento_invalido",
            f"Campo '{field_name}' exige no maximo 2 casas decimais: '{value}'.",
            field_name=field_name,
        )

    raw_digits = str(int(scaled))
    if raw_digits.startswith("-"):
        raise SerializationEncodingError(
            "valor_movimento_invalido",
            f"Campo '{field_name}' nao pode ser negativo: '{value}'.",
            field_name=field_name,
        )
    if len(raw_digits) > width:
        raise SerializationEncodingError(
            "campo_excede_largura",
            f"Campo '{field_name}' excede largura {width}: '{value}'.",
            field_name=field_name,
        )
    return raw_digits.zfill(width)


def _encoding_error_to_skip_code(exc: SerializationEncodingError) -> SerializationSkipCode:
    if exc.code == "campo_excede_largura":
        return SerializationSkipCode.FIELD_OVERFLOW
    if exc.code == "campo_numerico_invalido":
        return SerializationSkipCode.INVALID_NUMERIC_FIELD
    return SerializationSkipCode.INVALID_VALUE_PAYLOAD
