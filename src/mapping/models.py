"""Typed models for deterministic company-level mapping."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from domain import CanonicalMovement, PendingItem


class EmployeeResolutionSource(StrEnum):
    SNAPSHOT = "snapshot_canonico"
    SNAPSHOT_AND_CONFIG = "snapshot_e_config"
    CONFIG_EMPLOYEE_KEY = "config_por_chave"
    CONFIG_ALIAS = "config_por_alias"
    CONFLICT = "conflito_snapshot_config"
    AMBIGUOUS = "config_ambiguo"
    UNRESOLVED = "nao_resolvida"


class RubricResolutionSource(StrEnum):
    CONFIG_EVENT_MAPPING = "config_event_mapping"
    INACTIVE_MAPPING = "mapeamento_inativo"
    UNRESOLVED = "nao_resolvida"


class MappingStatus(StrEnum):
    READY = "pronto_para_serializer"
    BLOCKED = "bloqueado"


@dataclass(frozen=True, slots=True)
class AppliedConfigSummary:
    company_code: str
    company_name: str
    competence: str
    config_version: str
    default_process: str
    active_event_mappings: int
    active_employee_mappings: int


@dataclass(frozen=True, slots=True)
class SnapshotSummary:
    snapshot_version: str
    company_code: str
    company_name: str
    competence: str
    layout_version: str
    movement_count: int
    pending_count: int
    execution_status: str


@dataclass(frozen=True, slots=True)
class MappedMovement:
    canonical_movement: CanonicalMovement
    resolved_domain_registration: str | None
    employee_resolution_source: EmployeeResolutionSource
    output_rubric: str | None
    rubric_resolution_source: RubricResolutionSource
    mapping_pending_codes: tuple[str, ...] = ()
    mapping_pending_messages: tuple[str, ...] = ()
    status: MappingStatus = MappingStatus.READY

    def __post_init__(self) -> None:
        if self.status == MappingStatus.READY and self.canonical_movement.blocked:
            raise ValueError("MappedMovement cannot be ready when the canonical movement is already blocked.")
        if self.status == MappingStatus.READY and self.resolved_domain_registration is None:
            raise ValueError("MappedMovement ready state requires resolved_domain_registration.")
        if self.status == MappingStatus.READY and self.output_rubric is None:
            raise ValueError("MappedMovement ready state requires output_rubric.")

    @property
    def canonical_movement_id(self) -> str:
        return self.canonical_movement.movement_id

    @property
    def inherited_pending_codes(self) -> tuple[str, ...]:
        return self.canonical_movement.pending_codes

    @property
    def inherited_pending_messages(self) -> tuple[str, ...]:
        return self.canonical_movement.pending_messages


@dataclass(frozen=True, slots=True)
class MappingResult:
    snapshot: SnapshotSummary
    applied_config: AppliedConfigSummary
    mapped_movements: tuple[MappedMovement, ...]
    pendings: tuple[PendingItem, ...]

