"""Normalization helpers for template V1 ingestion."""

from __future__ import annotations

import re
from datetime import datetime, time
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from numbers import Real

from domain import NormalizedHours, PendingCode

from .errors import NormalizationError

_COMPETENCE_RE = re.compile(r"^(0[1-9]|1[0-2])/\d{4}$")


def validate_competence(value: str) -> str:
    text = _normalized_text(value)
    if text is None or not _COMPETENCE_RE.fullmatch(text):
        raise NormalizationError(
            PendingCode.COMPETENCE_INVALID,
            "Competencia invalida. Use o formato MM/AAAA.",
        )
    return text


def normalize_money_brl(value: object) -> Decimal:
    decimal_value = _parse_decimal_like(value, code=PendingCode.INVALID_VALUE)
    if decimal_value < 0:
        raise NormalizationError(PendingCode.INVALID_VALUE, "O valor monetario deve ser maior ou igual a zero.")
    return decimal_value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def normalize_quantity(value: object) -> Decimal:
    decimal_value = _parse_decimal_like(value, code=PendingCode.INVALID_QUANTITY)
    if decimal_value < 0:
        raise NormalizationError(PendingCode.INVALID_QUANTITY, "A quantidade deve ser maior ou igual a zero.")
    return decimal_value


def normalize_hours_hhmm(value: object) -> NormalizedHours:
    if isinstance(value, datetime):
        hour = value.hour
        minute = value.minute
    elif isinstance(value, time):
        hour = value.hour
        minute = value.minute
    else:
        text = _normalized_text(value)
        if text is None or not re.fullmatch(r"\d{2}:\d{2}", text):
            raise NormalizationError(
                PendingCode.INVALID_HOUR,
                "Hora invalida. Use o formato HH:MM.",
            )

        hour = int(text[:2])
        minute = int(text[3:])

    if minute > 59:
        raise NormalizationError(
            PendingCode.INVALID_HOUR,
            "Hora invalida. Os minutos devem estar entre 00 e 59.",
        )

    if hour < 0 or hour > 99:
        raise NormalizationError(
            PendingCode.INVALID_HOUR,
            "Hora invalida. As horas devem estar entre 00 e 99.",
        )

    return NormalizedHours(text=f"{hour:02d}:{minute:02d}", total_minutes=(hour * 60) + minute)


def is_empty_value(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    return False


def normalized_optional_text(value: object) -> str | None:
    return _normalized_text(value)


def _normalized_text(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    return str(value).strip() or None


def _parse_decimal_like(value: object, code: str) -> Decimal:
    if isinstance(value, Decimal):
        return value

    if isinstance(value, bool):
        raise NormalizationError(code, "Valor booleano nao e aceito nesta coluna.")

    if isinstance(value, int):
        return Decimal(value)

    if isinstance(value, Real):
        return Decimal(str(value))

    text = _normalized_text(value)
    if text is None:
        raise NormalizationError(code, "Valor vazio nao pode ser normalizado.")

    compact = text.replace(" ", "").replace("R$", "")
    sign = ""
    if compact.startswith("+"):
        compact = compact[1:]
    elif compact.startswith("-"):
        sign = "-"
        compact = compact[1:]

    if not compact:
        raise NormalizationError(code, "Valor vazio nao pode ser normalizado.")

    if "," in compact and "." in compact:
        integer_part, fraction_part = compact.rsplit(",", 1)
        if not re.fullmatch(r"\d{1,3}(\.\d{3})*", integer_part) or not fraction_part.isdigit():
            raise NormalizationError(code, f"Valor '{text}' nao pode ser interpretado com seguranca.")
        normalized = f"{integer_part.replace('.', '')}.{fraction_part}"
        return _decimal_from_string(sign + normalized, code, text)

    if "," in compact:
        integer_part, fraction_part = compact.split(",", 1)
        if not integer_part.isdigit() or not fraction_part.isdigit():
            raise NormalizationError(code, f"Valor '{text}' nao pode ser interpretado com seguranca.")
        return _decimal_from_string(f"{sign}{integer_part}.{fraction_part}", code, text)

    if "." in compact:
        integer_part, fraction_part = compact.split(".", 1)
        if not integer_part.isdigit() or not fraction_part.isdigit():
            raise NormalizationError(code, f"Valor '{text}' nao pode ser interpretado com seguranca.")
        if len(fraction_part) == 3:
            raise NormalizationError(code, f"Valor '{text}' e ambiguo e nao sera inferido automaticamente.")
        return _decimal_from_string(f"{sign}{integer_part}.{fraction_part}", code, text)

    if compact.isdigit():
        return _decimal_from_string(sign + compact, code, text)

    raise NormalizationError(code, f"Valor '{text}' nao pode ser interpretado com seguranca.")


def _decimal_from_string(value: str, code: str, original_text: str) -> Decimal:
    try:
        return Decimal(value)
    except InvalidOperation as exc:
        raise NormalizationError(
            code,
            f"Valor '{original_text}' nao pode ser interpretado com seguranca.",
        ) from exc
