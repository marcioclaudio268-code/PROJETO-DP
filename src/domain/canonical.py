"""Canonical in-memory models for payroll spreadsheet ingestion."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import StrEnum


class ValueType(StrEnum):
    MONETARY = "monetario"
    HOURS = "horas"
    DAYS = "dias"


class RegistrationSource(StrEnum):
    LINE = "linha_humana"
    REGISTRY = "cadastro_auxiliar"
    BOTH_MATCH = "linha_e_cadastro"
    CONFLICT = "conflito"
    UNRESOLVED = "nao_resolvida"


class PendingSeverity(StrEnum):
    LOW = "baixa"
    MEDIUM = "media"
    HIGH = "alta"
    BLOCKING = "bloqueante"


class PendingCode(StrEnum):
    PARAMETER_REQUIRED_MISSING = "parametro_obrigatorio_ausente"
    COMPETENCE_INVALID = "competencia_invalida"
    PAYROLL_TYPE_NOT_SUPPORTED = "tipo_folha_nao_suportado"
    EMPLOYEE_NOT_FOUND = "funcionario_nao_encontrado"
    DOMAIN_REGISTRATION_MISSING = "matricula_dominio_ausente"
    DOMAIN_REGISTRATION_DUPLICATED = "matricula_dominio_duplicada"
    DOMAIN_REGISTRATION_LINE_ONLY = "matricula_dominio_informada_somente_na_linha"
    DOMAIN_REGISTRATION_CONFLICT = "matricula_dominio_divergente"
    EMPLOYEE_REGISTRY_INCONSISTENT = "cadastro_funcionario_inconsistente"
    INVALID_HOUR = "hora_invalida"
    INVALID_VALUE = "valor_invalido"
    INVALID_QUANTITY = "quantidade_invalida"
    AMBIGUOUS_EVENT_NOTE = "evento_com_observacao_ambigua"
    NON_AUTOMATABLE_EVENT = "evento_nao_automatizavel"
    LINE_BLOCKED_BY_STATUS = "linha_bloqueada_por_status"


@dataclass(frozen=True, slots=True)
class SourceRef:
    sheet_name: str
    row_number: int
    cell: str
    column_name: str | None = None

    @property
    def full_cell_ref(self) -> str:
        return f"{self.sheet_name}!{self.cell}"


@dataclass(frozen=True, slots=True)
class NormalizedHours:
    text: str
    total_minutes: int


@dataclass(frozen=True, slots=True)
class PayrollFileParameters:
    company_code: str
    company_name: str
    competence: str
    payroll_type: str
    default_process: str
    layout_version: str
    source_cells: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ResolvedEmployee:
    employee_key: str | None
    employee_name: str | None
    domain_registration: str | None
    status: str | None
    allows_entries: bool | None
    source: SourceRef | None
    resolved_from_registry: bool
    registration_source: RegistrationSource = RegistrationSource.UNRESOLVED
    registry_consistent: bool = True

    def __post_init__(self) -> None:
        if self.registration_source == RegistrationSource.CONFLICT and self.domain_registration is not None:
            raise ValueError("ResolvedEmployee conflict state cannot carry a resolved domain_registration.")
        if self.registration_source == RegistrationSource.UNRESOLVED and self.domain_registration is not None:
            raise ValueError("ResolvedEmployee unresolved state cannot carry a resolved domain_registration.")


@dataclass(frozen=True, slots=True)
class PendingItem:
    pending_id: str
    severity: PendingSeverity
    company_code: str | None
    competence: str | None
    employee_key: str | None
    employee_name: str | None
    domain_registration: str | None
    event_name: str | None
    source: SourceRef
    pending_code: str
    description: str
    recommended_action: str
    treatment_status: str = "aberta"
    manual_resolution: str | None = None
    resolved_by: str | None = None
    resolved_at: str | None = None


@dataclass(frozen=True, slots=True)
class CanonicalMovement:
    movement_id: str
    company_code: str
    competence: str
    payroll_type: str
    default_process: str
    employee_key: str | None
    employee_name: str | None
    domain_registration: str | None
    event_name: str
    value_type: ValueType
    quantity: Decimal | None
    hours: NormalizedHours | None
    amount: Decimal | None
    source: SourceRef
    blocked: bool = False
    pending_codes: tuple[str, ...] = ()
    pending_messages: tuple[str, ...] = ()
    observation: str | None = None
    informed_rubric: str | None = None
    output_rubric: str | None = None
    event_nature: str | None = None
    serialization_unit: str | None = None

    def __post_init__(self) -> None:
        populated_fields = sum(
            value is not None for value in (self.quantity, self.hours, self.amount)
        )
        if populated_fields != 1:
            raise ValueError("CanonicalMovement must populate exactly one of quantity, hours or amount.")

        if self.value_type == ValueType.MONETARY and self.amount is None:
            raise ValueError("Monetary movement must populate amount.")
        if self.value_type == ValueType.HOURS and self.hours is None:
            raise ValueError("Hour movement must populate hours.")
        if self.value_type == ValueType.DAYS and self.quantity is None:
            raise ValueError("Day movement must populate quantity.")

    @property
    def has_pending(self) -> bool:
        return self.blocked or bool(self.pending_codes)

    def quantity_for_sheet(self) -> str | None:
        if self.hours is not None:
            return self.hours.text
        if self.quantity is not None:
            return decimal_to_plain_string(self.quantity)
        return None

    def amount_for_sheet(self) -> str | None:
        if self.amount is None:
            return None
        return decimal_to_plain_string(self.amount)

    def pending_codes_for_sheet(self) -> str | None:
        if not self.pending_codes:
            return None
        return "; ".join(self.pending_codes)

    def pending_messages_for_sheet(self) -> str | None:
        if not self.pending_messages:
            return None
        return " | ".join(self.pending_messages)


@dataclass(frozen=True, slots=True)
class IngestionResult:
    parameters: PayrollFileParameters
    employees: tuple[ResolvedEmployee, ...]
    movements: tuple[CanonicalMovement, ...]
    pendings: tuple[PendingItem, ...]


def decimal_to_plain_string(value: Decimal) -> str:
    """Serialize Decimal without scientific notation."""

    normalized = format(value, "f")
    if "." not in normalized:
        return normalized
    stripped = normalized.rstrip("0").rstrip(".")
    return stripped or "0"
