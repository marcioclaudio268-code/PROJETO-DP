"""Read-only TXT audit view for the operational dashboard."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from .models import DashboardRunResult


TXT_LINE_WIDTH = 43


@dataclass(frozen=True, slots=True)
class TxtAuditRubricTotal:
    rubric_raw: str
    rubric: str
    line_count: int
    value_type: str
    total_value: str | None
    total_reference: str | None
    display_total: str


@dataclass(frozen=True, slots=True)
class TxtAuditSummary:
    total_lines: int
    company_code: str
    company_name: str
    competence: str
    process_codes: tuple[str, ...]
    rubric_totals: tuple[TxtAuditRubricTotal, ...]


@dataclass(frozen=True, slots=True)
class TxtAuditEmployeeRow:
    line_number: int
    canonical_movement_id: str | None
    domain_registration_raw: str
    domain_registration: str
    employee_name: str | None
    rubric_raw: str
    rubric: str
    description: str
    value_type: str | None
    launched_value: str
    reference_raw: str
    value_raw: str
    txt_line: str
    check_status: str


@dataclass(frozen=True, slots=True)
class TxtAuditDivergence:
    code: str
    message: str
    line_number: int | None = None
    canonical_movement_id: str | None = None
    domain_registration: str | None = None
    rubric: str | None = None


@dataclass(frozen=True, slots=True)
class TxtAuditResult:
    summary: TxtAuditSummary
    employee_rows: tuple[TxtAuditEmployeeRow, ...]
    divergences: tuple[TxtAuditDivergence, ...]


@dataclass(frozen=True, slots=True)
class _ParsedTxtLine:
    line_number: int
    text: str
    tipo_registro: str
    domain_registration_raw: str
    competence_raw: str
    rubric_raw: str
    process_code_raw: str
    payload_raw: str
    company_code_raw: str

    @property
    def exact_key(self) -> tuple[str, str, str, str, str, str]:
        return (
            self.domain_registration_raw,
            self.competence_raw,
            self.rubric_raw,
            self.process_code_raw,
            self.payload_raw,
            self.company_code_raw,
        )

    @property
    def employee_rubric_key(self) -> tuple[str, str]:
        return (self.domain_registration_raw, self.rubric_raw)

    @property
    def employee_value_key(self) -> tuple[str, str]:
        return (
            self.domain_registration_raw,
            self.payload_raw,
        )


@dataclass(frozen=True, slots=True)
class _ExpectedMovement:
    canonical_movement_id: str
    domain_registration_raw: str
    competence_raw: str
    rubric_raw: str
    process_code_raw: str
    payload_raw: str
    company_code_raw: str
    employee_name: str | None
    event_name: str
    value_type: str
    launched_value: str

    @property
    def exact_key(self) -> tuple[str, str, str, str, str, str]:
        return (
            self.domain_registration_raw,
            self.competence_raw,
            self.rubric_raw,
            self.process_code_raw,
            self.payload_raw,
            self.company_code_raw,
        )

    @property
    def employee_rubric_key(self) -> tuple[str, str]:
        return (self.domain_registration_raw, self.rubric_raw)

    @property
    def employee_value_key(self) -> tuple[str, str]:
        return (
            self.domain_registration_raw,
            self.payload_raw,
        )


def build_txt_audit(result: DashboardRunResult) -> TxtAuditResult:
    """Build an in-memory audit view from existing dashboard artifacts only."""

    lines = _load_txt_lines(result)
    parsed_lines = tuple(_parse_txt_line(index, text) for index, text in enumerate(lines, start=1))
    expected_movements = tuple(_expected_movement(item) for item in result.mapped_payload.get("mapped_movements", ()))
    expected_movements = tuple(item for item in expected_movements if item is not None)

    expected_by_exact = _index_expected(expected_movements, "exact_key")
    expected_by_employee_rubric = _index_expected(expected_movements, "employee_rubric_key")
    expected_by_employee_value = _index_expected(expected_movements, "employee_value_key")
    employee_name_by_registration = _employee_name_by_registration(expected_movements)
    duplicate_counts = Counter(line.exact_key for line in parsed_lines)

    rows: list[TxtAuditEmployeeRow] = []
    divergences: list[TxtAuditDivergence] = []
    used_movement_ids: set[str] = set()

    for line in parsed_lines:
        row, row_divergences = _audit_txt_line(
            line,
            expected_by_exact=expected_by_exact,
            expected_by_employee_rubric=expected_by_employee_rubric,
            expected_by_employee_value=expected_by_employee_value,
            employee_name_by_registration=employee_name_by_registration,
            duplicate_count=duplicate_counts[line.exact_key],
            used_movement_ids=used_movement_ids,
        )
        rows.append(row)
        divergences.extend(row_divergences)

    return TxtAuditResult(
        summary=TxtAuditSummary(
            total_lines=len(parsed_lines),
            company_code=result.summary.company_code,
            company_name=result.summary.company_name,
            competence=result.summary.competence,
            process_codes=tuple(sorted({_display_code(line.process_code_raw) for line in parsed_lines})),
            rubric_totals=_rubric_totals(tuple(rows)),
        ),
        employee_rows=tuple(rows),
        divergences=tuple(divergences),
    )


def _load_txt_lines(result: DashboardRunResult) -> tuple[str, ...]:
    if not result.paths.txt_path.exists():
        return ()
    text = result.paths.txt_path.read_text(encoding="utf-8")
    return tuple(line for line in text.splitlines() if line)


def _parse_txt_line(line_number: int, text: str) -> _ParsedTxtLine:
    return _ParsedTxtLine(
        line_number=line_number,
        text=text,
        tipo_registro=text[0:1],
        domain_registration_raw=text[1:12],
        competence_raw=text[12:18],
        rubric_raw=text[18:22],
        process_code_raw=text[22:24],
        payload_raw=text[24:33],
        company_code_raw=text[33:43],
    )


def _expected_movement(payload: dict[str, Any]) -> _ExpectedMovement | None:
    if payload.get("status") != "pronto_para_serializer":
        return None
    domain_registration = payload.get("resolved_domain_registration")
    output_rubric = payload.get("output_rubric")
    if not domain_registration or not output_rubric:
        return None

    value_type = str(payload.get("value_type") or "")
    competence_raw = _encode_competence(payload.get("competence"))
    payload_raw = _expected_payload_raw(payload, value_type)
    if competence_raw is None or payload_raw is None:
        return None

    return _ExpectedMovement(
        canonical_movement_id=str(payload["canonical_movement_id"]),
        domain_registration_raw=_encode_numeric_identifier(domain_registration, width=11),
        competence_raw=competence_raw,
        rubric_raw=_encode_numeric_identifier(output_rubric, width=4),
        process_code_raw=_encode_numeric_identifier(payload.get("default_process"), width=2),
        payload_raw=payload_raw,
        company_code_raw=_encode_numeric_identifier(payload.get("company_code"), width=10),
        employee_name=_optional_text(payload.get("employee_name")),
        event_name=str(payload.get("event_name") or ""),
        value_type=value_type,
        launched_value=_movement_launched_value(payload, value_type),
    )


def _expected_payload_raw(payload: dict[str, Any], value_type: str) -> str | None:
    if value_type == "monetario":
        return _encode_implied_decimal(payload.get("amount"), width=9)
    if value_type == "horas":
        hours_payload = payload.get("hours") or {}
        hours_text = hours_payload.get("text")
        if not hours_text:
            return None
        parts = str(hours_text).split(":")
        if len(parts) != 2:
            return None
        return f"{int(parts[0]):02d}{int(parts[1]):02d}".zfill(9)
    if value_type in {"dias", "quantidade"}:
        return _encode_implied_decimal(payload.get("quantity"), width=9)
    return None


def _movement_launched_value(payload: dict[str, Any], value_type: str) -> str:
    if value_type == "monetario":
        return _decimal_display(payload.get("amount"))
    if value_type == "horas":
        hours_payload = payload.get("hours") or {}
        return str(hours_payload.get("text") or "")
    if value_type == "dias":
        return _decimal_display(payload.get("quantity"))
    return "-"


def _audit_txt_line(
    line: _ParsedTxtLine,
    *,
    expected_by_exact: dict[tuple, list[_ExpectedMovement]],
    expected_by_employee_rubric: dict[tuple, list[_ExpectedMovement]],
    expected_by_employee_value: dict[tuple, list[_ExpectedMovement]],
    employee_name_by_registration: dict[str, str],
    duplicate_count: int,
    used_movement_ids: set[str],
) -> tuple[TxtAuditEmployeeRow, list[TxtAuditDivergence]]:
    divergences: list[TxtAuditDivergence] = []
    status = "OK"
    expected = _pick_unique_unused(expected_by_exact.get(line.exact_key, ()), used_movement_ids)

    if len(line.text) != TXT_LINE_WIDTH:
        status = "LINHA_TXT_INVALIDA"
        divergences.append(
            _divergence(
                "LINHA_TXT_INVALIDA",
                "Linha do TXT possui tamanho diferente de 43 posicoes.",
                line,
                None,
            )
        )
        return (
            TxtAuditEmployeeRow(
                line_number=line.line_number,
                canonical_movement_id=None,
                domain_registration_raw=line.domain_registration_raw,
                domain_registration=_display_code(line.domain_registration_raw),
                employee_name=employee_name_by_registration.get(line.domain_registration_raw),
                rubric_raw=line.rubric_raw,
                rubric=_display_code(line.rubric_raw),
                description="Linha TXT invalida",
                value_type=None,
                launched_value=_line_launched_value(line),
                reference_raw=line.payload_raw,
                value_raw=line.payload_raw,
                txt_line=line.text,
                check_status=status,
            ),
            divergences,
        )

    if expected is None:
        exact_candidates = expected_by_exact.get(line.exact_key, ())
        if len(exact_candidates) > 1:
            status = "MULTIPLA_CORRESPONDENCIA"
            expected = exact_candidates[0]
            divergences.append(
                _divergence(
                    "MULTIPLA_CORRESPONDENCIA",
                    "Linha do TXT possui mais de um movimento esperado compativel.",
                    line,
                    expected,
                )
            )
        else:
            expected, status = _classify_unmatched_line(
                line,
                expected_by_employee_rubric=expected_by_employee_rubric,
                expected_by_employee_value=expected_by_employee_value,
            )
            divergences.append(_divergence(status, _message_for_status(status), line, expected))

    if expected is not None:
        used_movement_ids.add(expected.canonical_movement_id)

    if duplicate_count > 1:
        status = "DUPLICADO"
        divergences.append(
            _divergence(
                "DUPLICADO",
                "Mesma combinacao matricula + rubrica + referencia + valor aparece mais de uma vez no TXT.",
                line,
                expected,
            )
        )

    employee_name = expected.employee_name if expected is not None else employee_name_by_registration.get(line.domain_registration_raw)
    if expected is not None and not employee_name:
        if status == "OK":
            status = "MATRICULA_SEM_NOME"
        divergences.append(
            _divergence(
                "MATRICULA_SEM_NOME",
                "Movimento reconciliado, mas a matricula nao possui nome associado no artefato mapeado.",
                line,
                expected,
            )
        )

    return (
        TxtAuditEmployeeRow(
            line_number=line.line_number,
            canonical_movement_id=(expected.canonical_movement_id if expected is not None else None),
            domain_registration_raw=line.domain_registration_raw,
            domain_registration=_display_code(line.domain_registration_raw),
            employee_name=employee_name,
            rubric_raw=line.rubric_raw,
            rubric=_display_code(line.rubric_raw),
            description=(expected.event_name if expected is not None else "Nao reconciliado"),
            value_type=(expected.value_type if expected is not None else None),
            launched_value=(expected.launched_value if expected is not None else _line_launched_value(line)),
            reference_raw=line.payload_raw,
            value_raw=line.payload_raw,
            txt_line=line.text,
            check_status=status,
        ),
        divergences,
    )


def _classify_unmatched_line(
    line: _ParsedTxtLine,
    *,
    expected_by_employee_rubric: dict[tuple, list[_ExpectedMovement]],
    expected_by_employee_value: dict[tuple, list[_ExpectedMovement]],
) -> tuple[_ExpectedMovement | None, str]:
    value_candidates = expected_by_employee_rubric.get(line.employee_rubric_key, ())
    if value_candidates:
        return value_candidates[0], "VALOR_DIVERGENTE"

    rubric_candidates = expected_by_employee_value.get(line.employee_value_key, ())
    if rubric_candidates:
        return rubric_candidates[0], "RUBRICA_DIVERGENTE"

    return None, "NAO_LOCALIZADO_NA_FOLHA"


def _pick_unique_unused(
    candidates: list[_ExpectedMovement] | tuple[_ExpectedMovement, ...],
    used_movement_ids: set[str],
) -> _ExpectedMovement | None:
    unused = [item for item in candidates if item.canonical_movement_id not in used_movement_ids]
    if len(unused) == 1:
        return unused[0]
    return None


def _index_expected(expected_movements: tuple[_ExpectedMovement, ...], attribute: str) -> dict[tuple, list[_ExpectedMovement]]:
    index: dict[tuple, list[_ExpectedMovement]] = defaultdict(list)
    for movement in expected_movements:
        index[getattr(movement, attribute)].append(movement)
    return index


def _employee_name_by_registration(expected_movements: tuple[_ExpectedMovement, ...]) -> dict[str, str]:
    names: dict[str, str] = {}
    for movement in expected_movements:
        if movement.employee_name:
            names.setdefault(movement.domain_registration_raw, movement.employee_name)
    return names


def _rubric_totals(rows: tuple[TxtAuditEmployeeRow, ...]) -> tuple[TxtAuditRubricTotal, ...]:
    grouped: dict[str, list[TxtAuditEmployeeRow]] = defaultdict(list)
    for row in rows:
        grouped[row.rubric_raw].append(row)

    totals: list[TxtAuditRubricTotal] = []
    for rubric_raw, rubric_rows in sorted(grouped.items()):
        value_types = {row.value_type for row in rubric_rows if row.value_type is not None}
        value_type = next(iter(value_types)) if len(value_types) == 1 else ("misto" if value_types else "nao_classificado")
        total_value: str | None = None
        total_reference: str | None = None

        if value_type == "monetario":
            total = sum((_decode_implied_decimal(row.value_raw) for row in rubric_rows), Decimal("0"))
            total_value = _decimal_display(total)
            display_total = total_value
        elif value_type == "horas":
            total_minutes = sum(_decode_hours_reference(row.reference_raw) for row in rubric_rows)
            total_reference = _minutes_to_hhmm(total_minutes)
            display_total = total_reference
        elif value_type in {"dias", "quantidade"}:
            total = sum((_decode_implied_decimal(row.reference_raw) for row in rubric_rows), Decimal("0"))
            total_reference = _decimal_display(total)
            display_total = f"{total_reference} dia(s)"
        elif value_type == "misto":
            display_total = "misto; revisar linhas"
        else:
            display_total = "nao classificado"

        totals.append(
            TxtAuditRubricTotal(
                rubric_raw=rubric_raw,
                rubric=_display_code(rubric_raw),
                line_count=len(rubric_rows),
                value_type=value_type,
                total_value=total_value,
                total_reference=total_reference,
                display_total=display_total,
            )
        )
    return tuple(totals)


def _divergence(
    code: str,
    message: str,
    line: _ParsedTxtLine,
    expected: _ExpectedMovement | None,
) -> TxtAuditDivergence:
    return TxtAuditDivergence(
        code=code,
        message=message,
        line_number=line.line_number,
        canonical_movement_id=(expected.canonical_movement_id if expected is not None else None),
        domain_registration=_display_code(line.domain_registration_raw),
        rubric=_display_code(line.rubric_raw),
    )


def _message_for_status(status: str) -> str:
    if status == "VALOR_DIVERGENTE":
        return "Matricula e rubrica existem no movimento esperado, mas referencia/valor do TXT diverge."
    if status == "RUBRICA_DIVERGENTE":
        return "Matricula e referencia/valor existem no movimento esperado, mas rubrica do TXT diverge."
    return "Linha do TXT nao foi localizada entre os movimentos mapeados esperados."


def _line_launched_value(line: _ParsedTxtLine) -> str:
    return f"payload={line.payload_raw}"


def _encode_numeric_identifier(value: object, *, width: int) -> str:
    text = str(value or "").strip()
    return text.zfill(width)


def _encode_implied_decimal(value: object, *, width: int) -> str | None:
    try:
        decimal_value = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    scaled = decimal_value * 100
    if scaled != scaled.to_integral_value():
        return None
    return str(int(scaled)).zfill(width)


def _decode_implied_decimal(value: str) -> Decimal:
    return Decimal(int(value or "0")) / Decimal("100")


def _decode_hours_reference(value: str) -> int:
    text = (value or "").zfill(9)
    hour_minute = text[-4:]
    if not hour_minute.isdigit():
        return 0
    return int(hour_minute[:2]) * 60 + int(hour_minute[2:])


def _encode_competence(value: object) -> str | None:
    text = _optional_text(value)
    if text is None:
        return None
    if len(text) == 6 and text.isdigit():
        month = int(text[:2])
        if 1 <= month <= 12:
            return f"{text[2:]}{text[:2]}"
        return text
    if len(text) == 7 and text[2] == "/":
        month, year = text.split("/")
        if len(month) == 2 and len(year) == 4 and month.isdigit() and year.isdigit():
            return f"{year}{month}"
    return None


def _minutes_to_hhmm(total_minutes: int) -> str:
    hours, minutes = divmod(total_minutes, 60)
    return f"{hours:02d}:{minutes:02d}"


def _display_code(value: str) -> str:
    return value.lstrip("0") or "0"


def _decimal_display(value: object) -> str:
    try:
        decimal_value = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return str(value or "")
    normalized = format(decimal_value, "f")
    if "." not in normalized:
        return normalized
    return normalized.rstrip("0").rstrip(".") or "0"


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


__all__ = [
    "TxtAuditDivergence",
    "TxtAuditEmployeeRow",
    "TxtAuditResult",
    "TxtAuditRubricTotal",
    "TxtAuditSummary",
    "build_txt_audit",
]
