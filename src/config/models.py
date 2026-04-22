"""Pydantic models for the payroll TXT engine configuration layer."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Sequence

from pydantic import BaseModel, ConfigDict, Field, model_validator


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _duplicate_values(values: Sequence[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    duplicates: set[str] = set()

    for value in values:
        if value in seen:
            duplicates.add(value)
        else:
            seen.add(value)

    return tuple(sorted(duplicates))


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class EventMapping(StrictModel):
    """Map one business event to one outbound Dominio rubric."""

    event_negocio: str = Field(..., min_length=1, description="Codigo do evento de negocio")
    rubrica_saida: str = Field(..., min_length=1, description="Rubrica de saida no TXT")
    active: bool = Field(default=True, description="Marks the mapping as active")
    notes: str | None = Field(default=None, description="Free text notes")


class EmployeeMapping(StrictModel):
    """Resolve one source key to one Dominio registration."""

    source_employee_key: str = Field(..., min_length=1, description="Chave estavel de origem")
    domain_registration: str = Field(..., min_length=1, description="Matricula no Dominio")
    source_employee_name: str | None = Field(default=None, description="Nome legivel da origem")
    aliases: list[str] = Field(default_factory=list, description="Chaves alternativas aceitas")
    active: bool = Field(default=True, description="Marks the mapping as active")
    notes: str | None = Field(default=None, description="Free text notes")


class PendingPolicy(StrictModel):
    """Rules that force review or create a pending item."""

    review_required_event_negocios: list[str] = Field(
        default_factory=list,
        description="Business events that must produce pending review",
    )
    review_required_fields: list[str] = Field(
        default_factory=list,
        description="Canonical fields that force review when unresolved",
    )
    block_on_ambiguous_observations: bool = Field(
        default=True,
        description="Ambiguous observations cannot be exported silently",
    )
    block_on_unmapped_employee: bool = Field(
        default=True,
        description="Missing employee mapping blocks export",
    )
    block_on_unmapped_event: bool = Field(
        default=True,
        description="Missing event mapping blocks export",
    )
    notes: str | None = Field(default=None, description="Free text notes")


class CompanyConfig(StrictModel):
    """Versioned configuration for one company/branch scope."""

    company_code: str = Field(..., min_length=1, description="Company or branch code")
    company_name: str = Field(..., min_length=1, description="Display name")
    default_process: str = Field(..., min_length=1, description="Default process code")
    competence: str = Field(..., min_length=1, description="Target competence")
    config_version: str = Field(..., min_length=1, description="Configuration version")
    event_mappings: list[EventMapping] = Field(default_factory=list)
    employee_mappings: list[EmployeeMapping] = Field(default_factory=list)
    pending_policy: PendingPolicy = Field(default_factory=PendingPolicy)
    validation_flags: dict[str, bool] = Field(
        default_factory=dict,
        description="Extensible validation switches",
    )
    notes: str | None = Field(default=None, description="Free text notes")

    @model_validator(mode="after")
    def _check_duplicate_mappings(self) -> "CompanyConfig":
        event_codes = [mapping.event_negocio for mapping in self.event_mappings]
        employee_keys = [mapping.source_employee_key for mapping in self.employee_mappings]

        duplicate_events = _duplicate_values(event_codes)
        duplicate_employees = _duplicate_values(employee_keys)

        errors: list[str] = []
        if duplicate_events:
            errors.append(f"duplicate event_negocio values: {', '.join(duplicate_events)}")
        if duplicate_employees:
            errors.append(
                f"duplicate source_employee_key values: {', '.join(duplicate_employees)}"
            )

        if errors:
            raise ValueError("; ".join(errors))

        return self


class RunManifest(StrictModel):
    """Minimum execution manifest for traceability."""

    run_id: str = Field(..., min_length=1, description="Stable execution id")
    engine_version: str = Field(..., min_length=1, description="Engine version")
    company_code: str = Field(..., min_length=1, description="Company or branch code")
    company_name: str | None = Field(default=None, description="Display name")
    competence: str = Field(..., min_length=1, description="Target competence")
    config_version: str | None = Field(default=None, min_length=1, description="Configuration version")
    layout_version: str | None = Field(default=None, min_length=1, description="Layout version")
    generated_at: datetime = Field(default_factory=_utc_now)
    artifact_hashes: dict[str, str] = Field(
        default_factory=dict,
        description="Hashes for input/output artifacts",
    )
    movement_count: int = Field(default=0, ge=0, description="Generated movement count")
    pending_count: int = Field(default=0, ge=0, description="Pending item count")
    status: str = Field(default="draft", min_length=1, description="Execution status")

    @model_validator(mode="after")
    def _check_version_identifier(self) -> "RunManifest":
        if not self.config_version and not self.layout_version:
            raise ValueError("RunManifest requires config_version or layout_version.")
        return self


class CompanyRegistryEntry(StrictModel):
    """Master company registry entry used by the internal resolver."""

    id: str = Field(..., min_length=1, description="Stable registry id")
    company_code: str = Field(..., min_length=1, description="Primary company code")
    cnpj: str | None = Field(default=None, description="Company CNPJ, normalized as digits")
    razao_social: str | None = Field(default=None, description="Registered company name")
    nome_fantasia: str | None = Field(default=None, description="Trading name")
    status: str = Field(default="active", min_length=1, description="Registry status")
    is_active: bool = Field(default=True, description="Marks the company as active")
    default_template_id: str | None = Field(default=None, description="Optional template id")
    active_config_id: str | None = Field(default=None, description="Linked active config id")
    last_competence_seen: str | None = Field(default=None, description="Most recent competence seen")
    source_import: str = Field(default="resumo_mensal", min_length=1, description="Import source")
    created_at: datetime = Field(default_factory=_utc_now)
    updated_at: datetime = Field(default_factory=_utc_now)


class CompanyConfigRecord(StrictModel):
    """Internal config record linked to one registry company."""

    id: str = Field(..., min_length=1, description="Stable config record id")
    company_id: str = Field(..., min_length=1, description="Linked registry id")
    version: str = Field(..., min_length=1, description="Internal config version")
    competence_start: str | None = Field(default=None, description="Optional competence start")
    competence_end: str | None = Field(default=None, description="Optional competence end")
    status: str = Field(default="active", min_length=1, description="Config status")
    config_payload_internal: dict[str, Any] = Field(default_factory=dict, description="Validated config payload")
    validated_at: datetime | None = Field(default=None)
    created_at: datetime = Field(default_factory=_utc_now)
    updated_at: datetime = Field(default_factory=_utc_now)


class CompanyConfigIssue(StrictModel):
    """Internal issue created during registry/import maintenance."""

    id: str = Field(..., min_length=1, description="Stable issue id")
    company_id: str = Field(..., min_length=1, description="Linked registry id")
    issue_type: str = Field(..., min_length=1, description="Issue code")
    description: str = Field(..., min_length=1, description="Human readable description")
    status: str = Field(default="open", min_length=1, description="Issue status")
    created_at: datetime = Field(default_factory=_utc_now)
    updated_at: datetime = Field(default_factory=_utc_now)


class MasterDataImportResult(StrictModel):
    """Summary of a Resumo Mensal import into the internal company master data."""

    source_path: str = Field(..., min_length=1, description="Imported workbook path")
    sheet_name: str | None = Field(default=None, description="Imported sheet name")
    rows_read: int = Field(default=0, ge=0)
    sections_read: int = Field(default=0, ge=0)
    companies_seen: int = Field(default=0, ge=0)
    duplicate_company_sections_ignored: int = Field(default=0, ge=0)
    companies_created: int = Field(default=0, ge=0)
    companies_updated: int = Field(default=0, ge=0)
    configs_created: int = Field(default=0, ge=0)
    configs_updated: int = Field(default=0, ge=0)
    issues_created: int = Field(default=0, ge=0)
    detected_fields: list[str] = Field(default_factory=list, description="Detected workbook fields")
    parse_mode: str = Field(default="tabular", min_length=1, description="Import parsing mode")
    registry_path: str = Field(..., min_length=1, description="Registry JSON path")
    configs_path: str = Field(..., min_length=1, description="Configs JSON path")
    issues_path: str = Field(..., min_length=1, description="Issues JSON path")
    message: str = Field(default="", description="Summary message")
