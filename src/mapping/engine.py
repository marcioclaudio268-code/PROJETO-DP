"""Pure deterministic mapping from canonical ingestion result to company-mapped output."""

from __future__ import annotations

from dataclasses import dataclass

from config import CompanyConfig, EmployeeMapping, EventMapping
from domain import IngestionResult, PendingItem, PendingSeverity

from .errors import MappingConfigurationError
from .models import (
    AppliedConfigSummary,
    EmployeeResolutionSource,
    MappedMovement,
    MappingResult,
    MappingStatus,
    RubricResolutionSource,
    SnapshotSummary,
)
from .taxonomy import (
    MappingFatalCode,
    MappingPendingCode,
    render_mapping_fatal_message,
    render_mapping_pending_definition,
)


@dataclass(frozen=True, slots=True)
class _EmployeeCandidate:
    mapping: EmployeeMapping
    resolution_source: EmployeeResolutionSource


def map_ingestion_result(
    ingestion_result: IngestionResult,
    company_config: CompanyConfig,
    *,
    snapshot_summary: SnapshotSummary,
) -> MappingResult:
    _validate_company_scope(ingestion_result, company_config)

    employee_index = _build_employee_index(company_config.employee_mappings)
    event_index = {mapping.event_negocio: mapping for mapping in company_config.event_mappings}

    mapped_movements: list[MappedMovement] = []
    mapping_pendings: list[PendingItem] = []
    pending_counter = 1

    for movement in ingestion_result.movements:
        resolved_registration, employee_source, employee_pendings, pending_counter = (
            _resolve_domain_registration(
                movement=movement,
                company_config=company_config,
                employee_index=employee_index,
                pending_counter=pending_counter,
            )
        )
        output_rubric, rubric_source, rubric_pendings, pending_counter = _resolve_output_rubric(
            movement=movement,
            company_config=company_config,
            event_index=event_index,
            pending_counter=pending_counter,
        )
        policy_pendings, pending_counter = _build_policy_pendings(
            movement=movement,
            company_config=company_config,
            pending_counter=pending_counter,
        )

        movement_pendings = (*employee_pendings, *rubric_pendings, *policy_pendings)
        has_blocking_pending = any(
            pending.severity == PendingSeverity.BLOCKING for pending in movement_pendings
        )
        movement_ready = (
            not movement.blocked
            and not has_blocking_pending
            and resolved_registration is not None
            and output_rubric is not None
        )
        status = MappingStatus.READY if movement_ready else MappingStatus.BLOCKED

        mapped_movements.append(
            MappedMovement(
                canonical_movement=movement,
                resolved_domain_registration=resolved_registration,
                employee_resolution_source=employee_source,
                output_rubric=output_rubric,
                rubric_resolution_source=rubric_source,
                mapping_pending_codes=tuple(pending.pending_code for pending in movement_pendings),
                mapping_pending_messages=tuple(pending.description for pending in movement_pendings),
                status=status,
            )
        )
        mapping_pendings.extend(movement_pendings)

    return MappingResult(
        snapshot=snapshot_summary,
        applied_config=_build_config_summary(company_config),
        mapped_movements=tuple(mapped_movements),
        pendings=tuple(mapping_pendings),
    )


def summarize_mapping_result(result: MappingResult) -> dict[str, int]:
    return {
        "mapped_movements": len(result.mapped_movements),
        "ready_movements": sum(
            1 for movement in result.mapped_movements if movement.status == MappingStatus.READY
        ),
        "blocked_movements": sum(
            1 for movement in result.mapped_movements if movement.status == MappingStatus.BLOCKED
        ),
        "mapping_pendings": len(result.pendings),
        "blocking_mapping_pendings": sum(
            1 for pending in result.pendings if pending.severity == PendingSeverity.BLOCKING
        ),
    }


def infer_mapping_execution_status(result: MappingResult) -> str:
    if any(movement.status == MappingStatus.BLOCKED for movement in result.mapped_movements):
        return "blocked"
    if result.pendings:
        return "success_with_pending"
    return "success"


def _validate_company_scope(
    ingestion_result: IngestionResult,
    company_config: CompanyConfig,
) -> None:
    parameters = ingestion_result.parameters

    if parameters.company_code != company_config.company_code:
        raise MappingConfigurationError(
            MappingFatalCode.SNAPSHOT_COMPANY_MISMATCH,
            render_mapping_fatal_message(
                MappingFatalCode.SNAPSHOT_COMPANY_MISMATCH,
                snapshot_company_code=parameters.company_code,
                config_company_code=company_config.company_code,
            ),
        )

    if parameters.competence != company_config.competence:
        raise MappingConfigurationError(
            MappingFatalCode.SNAPSHOT_COMPETENCE_MISMATCH,
            render_mapping_fatal_message(
                MappingFatalCode.SNAPSHOT_COMPETENCE_MISMATCH,
                snapshot_competence=parameters.competence,
                config_competence=company_config.competence,
            ),
        )


def _build_config_summary(company_config: CompanyConfig) -> AppliedConfigSummary:
    return AppliedConfigSummary(
        company_code=company_config.company_code,
        company_name=company_config.company_name,
        competence=company_config.competence,
        config_version=company_config.config_version,
        default_process=company_config.default_process,
        active_event_mappings=sum(1 for mapping in company_config.event_mappings if mapping.active),
        active_employee_mappings=sum(1 for mapping in company_config.employee_mappings if mapping.active),
    )


def _build_employee_index(
    employee_mappings: list[EmployeeMapping],
) -> dict[str, list[_EmployeeCandidate]]:
    index: dict[str, list[_EmployeeCandidate]] = {}

    for mapping in employee_mappings:
        if not mapping.active:
            continue

        index.setdefault(mapping.source_employee_key, []).append(
            _EmployeeCandidate(mapping, EmployeeResolutionSource.CONFIG_EMPLOYEE_KEY)
        )
        for alias in mapping.aliases:
            index.setdefault(alias, []).append(
                _EmployeeCandidate(mapping, EmployeeResolutionSource.CONFIG_ALIAS)
            )

    return index


def _resolve_domain_registration(
    *,
    movement,
    company_config: CompanyConfig,
    employee_index: dict[str, list[_EmployeeCandidate]],
    pending_counter: int,
) -> tuple[str | None, EmployeeResolutionSource, tuple[PendingItem, ...], int]:
    employee_key = movement.employee_key
    snapshot_registration = movement.domain_registration
    candidates = employee_index.get(employee_key, []) if employee_key else []
    unique_registrations = tuple(sorted({candidate.mapping.domain_registration for candidate in candidates}))

    if not candidates:
        if snapshot_registration is not None:
            return snapshot_registration, EmployeeResolutionSource.SNAPSHOT, (), pending_counter

        pending = _make_mapping_pending(
            pending_counter=pending_counter,
            company_code=movement.company_code,
            competence=movement.competence,
            employee_key=employee_key,
            employee_name=movement.employee_name,
            domain_registration=None,
            event_name=movement.event_name,
            source=movement.source,
            code=MappingPendingCode.EMPLOYEE_MAPPING_MISSING,
            severity_override=(
                PendingSeverity.BLOCKING
                if company_config.pending_policy.block_on_unmapped_employee
                else PendingSeverity.HIGH
            ),
            employee_key_label=employee_key or "<ausente>",
        )
        return None, EmployeeResolutionSource.UNRESOLVED, (pending,), pending_counter + 1

    if len(unique_registrations) > 1:
        pending = _make_mapping_pending(
            pending_counter=pending_counter,
            company_code=movement.company_code,
            competence=movement.competence,
            employee_key=employee_key,
            employee_name=movement.employee_name,
            domain_registration=snapshot_registration,
            event_name=movement.event_name,
            source=movement.source,
            code=MappingPendingCode.EMPLOYEE_MAPPING_AMBIGUOUS,
            employee_key_label=employee_key or "<ausente>",
            candidate_registrations=", ".join(unique_registrations),
        )
        return None, EmployeeResolutionSource.AMBIGUOUS, (pending,), pending_counter + 1

    resolved_from_config = unique_registrations[0]
    resolution_source = _preferred_employee_resolution_source(candidates)

    if snapshot_registration is None:
        return resolved_from_config, resolution_source, (), pending_counter

    if snapshot_registration == resolved_from_config:
        return snapshot_registration, EmployeeResolutionSource.SNAPSHOT_AND_CONFIG, (), pending_counter

    pending = _make_mapping_pending(
        pending_counter=pending_counter,
        company_code=movement.company_code,
        competence=movement.competence,
        employee_key=employee_key,
        employee_name=movement.employee_name,
        domain_registration=snapshot_registration,
        event_name=movement.event_name,
        source=movement.source,
        code=MappingPendingCode.EMPLOYEE_MAPPING_CONFLICT,
        snapshot_registration=snapshot_registration,
        config_registration=resolved_from_config,
        employee_key_label=employee_key or "<ausente>",
    )
    return None, EmployeeResolutionSource.CONFLICT, (pending,), pending_counter + 1


def _preferred_employee_resolution_source(
    candidates: list[_EmployeeCandidate],
) -> EmployeeResolutionSource:
    if any(candidate.resolution_source == EmployeeResolutionSource.CONFIG_EMPLOYEE_KEY for candidate in candidates):
        return EmployeeResolutionSource.CONFIG_EMPLOYEE_KEY
    return EmployeeResolutionSource.CONFIG_ALIAS


def _resolve_output_rubric(
    *,
    movement,
    company_config: CompanyConfig,
    event_index: dict[str, EventMapping],
    pending_counter: int,
) -> tuple[str | None, RubricResolutionSource, tuple[PendingItem, ...], int]:
    event_mapping = event_index.get(movement.event_name)

    if event_mapping is None:
        pending = _make_mapping_pending(
            pending_counter=pending_counter,
            company_code=movement.company_code,
            competence=movement.competence,
            employee_key=movement.employee_key,
            employee_name=movement.employee_name,
            domain_registration=movement.domain_registration,
            event_name=movement.event_name,
            source=movement.source,
            code=MappingPendingCode.EVENT_MAPPING_MISSING,
            severity_override=(
                PendingSeverity.BLOCKING
                if company_config.pending_policy.block_on_unmapped_event
                else PendingSeverity.HIGH
            ),
        )
        return None, RubricResolutionSource.UNRESOLVED, (pending,), pending_counter + 1

    if not event_mapping.active:
        pending = _make_mapping_pending(
            pending_counter=pending_counter,
            company_code=movement.company_code,
            competence=movement.competence,
            employee_key=movement.employee_key,
            employee_name=movement.employee_name,
            domain_registration=movement.domain_registration,
            event_name=movement.event_name,
            source=movement.source,
            code=MappingPendingCode.EVENT_MAPPING_INACTIVE,
            severity_override=(
                PendingSeverity.BLOCKING
                if company_config.pending_policy.block_on_unmapped_event
                else PendingSeverity.HIGH
            ),
        )
        return None, RubricResolutionSource.INACTIVE_MAPPING, (pending,), pending_counter + 1

    return event_mapping.rubrica_saida, RubricResolutionSource.CONFIG_EVENT_MAPPING, (), pending_counter


def _build_policy_pendings(
    *,
    movement,
    company_config: CompanyConfig,
    pending_counter: int,
) -> tuple[tuple[PendingItem, ...], int]:
    if movement.event_name not in company_config.pending_policy.review_required_event_negocios:
        return (), pending_counter

    pending = _make_mapping_pending(
        pending_counter=pending_counter,
        company_code=movement.company_code,
        competence=movement.competence,
        employee_key=movement.employee_key,
        employee_name=movement.employee_name,
        domain_registration=movement.domain_registration,
        event_name=movement.event_name,
        source=movement.source,
        code=MappingPendingCode.EVENT_REVIEW_REQUIRED,
    )
    return (pending,), pending_counter + 1


def _make_mapping_pending(
    *,
    pending_counter: int,
    company_code: str,
    competence: str,
    employee_key: str | None,
    employee_name: str | None,
    domain_registration: str | None,
    event_name: str | None,
    source,
    code: MappingPendingCode,
    severity_override: PendingSeverity | None = None,
    **context: str,
) -> PendingItem:
    severity, description, recommended_action = render_mapping_pending_definition(
        code,
        employee_key=employee_key or "",
        event_name=event_name or "",
        snapshot_registration=context.get("snapshot_registration", ""),
        config_registration=context.get("config_registration", ""),
        candidate_registrations=context.get("candidate_registrations", ""),
        employee_key_label=context.get("employee_key_label", employee_key or ""),
    )

    return PendingItem(
        pending_id=f"map-pend-{pending_counter:05d}",
        severity=severity_override or severity,
        company_code=company_code,
        competence=competence,
        employee_key=employee_key,
        employee_name=employee_name,
        domain_registration=domain_registration,
        event_name=event_name,
        source=source,
        pending_code=code,
        description=description,
        recommended_action=recommended_action,
    )

