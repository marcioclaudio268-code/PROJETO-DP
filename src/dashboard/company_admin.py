"""Administrative helpers for company setup in the local dashboard."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from config import (
    CompanyConfig,
    CompanyConfigRecord,
    CompanyMasterDataStore,
    CompanyRegistryEntry,
)
from config.master_data import (
    company_config_id,
    company_registry_id,
    competence_sort_key,
    normalize_competence,
    normalize_text,
)

from .column_mapping_profiles import (
    ColumnGenerationMode,
    ColumnMappingProfileError,
    ColumnMappingRule,
    CompanyColumnMappingProfile,
    load_column_mapping_profile,
    save_column_mapping_profile,
    upsert_column_mapping_rule,
)
from .company_employee_registry import (
    CompanyEmployeeRecord,
    load_company_employee_registry,
    save_company_employee_registry,
    upsert_employee_record,
)
from .company_rubric_catalog import (
    CompanyRubricRecord,
    load_company_rubric_catalog,
    save_company_rubric_catalog,
    upsert_rubric_record,
)


@dataclass(frozen=True, slots=True)
class CompanyAdminEntry:
    company_code: str
    company_name: str
    status: str
    is_active: bool
    company_id: str
    active_config_id: str | None = None
    default_process: str | None = None
    competence: str | None = None
    config_version: str | None = None
    config_status: str | None = None

    def selection_label(self) -> str:
        return f"{self.company_code} - {self.company_name}"


def list_company_admin_entries(
    *,
    root: str | Path | None = None,
) -> tuple[CompanyAdminEntry, ...]:
    store = CompanyMasterDataStore(root)
    registry_entries = list(store.load_registry_entries())
    config_records = list(store.load_config_records())
    records_by_company: dict[str, list[CompanyConfigRecord]] = {}
    for record in config_records:
        records_by_company.setdefault(record.company_id, []).append(record)

    entries: list[CompanyAdminEntry] = []
    seen_company_ids: set[str] = set()
    for company in registry_entries:
        records = records_by_company.get(company.id, [])
        selected_record = _select_display_config_record(company, records)
        entries.append(_admin_entry_from_registry(company, selected_record))
        seen_company_ids.add(company.id)

    for record in config_records:
        if record.company_id in seen_company_ids:
            continue
        entry = _admin_entry_from_config_record(record)
        if entry is not None:
            entries.append(entry)
            seen_company_ids.add(record.company_id)

    return tuple(sorted(entries, key=_company_entry_sort_key))


def get_company_admin_entry(
    company_code: str,
    *,
    root: str | Path | None = None,
) -> CompanyAdminEntry | None:
    normalized_code = _required_text("company_code", company_code)
    for entry in list_company_admin_entries(root=root):
        if entry.company_code == normalized_code:
            return entry
    return None


def save_company_admin_entry(
    *,
    company_code: str,
    company_name: str,
    default_process: str,
    competence: str | None = None,
    is_active: bool = True,
    root: str | Path | None = None,
) -> CompanyAdminEntry:
    code = _required_text("company_code", company_code)
    name = _required_text("company_name", company_name)
    process = _required_text("default_process", default_process)
    normalized_competence = _optional_competence(competence)
    status = "active" if is_active else "inactive"

    store = CompanyMasterDataStore(root)
    now = _utc_now()
    existing_company = store.find_company_by_code(code)
    company_id = existing_company.id if existing_company is not None else company_registry_id(code, None)

    company = CompanyRegistryEntry(
        id=company_id,
        company_code=code,
        cnpj=existing_company.cnpj if existing_company is not None else None,
        razao_social=name,
        nome_fantasia=(existing_company.nome_fantasia if existing_company is not None else None) or name,
        status=status,
        is_active=is_active,
        default_template_id=(
            existing_company.default_template_id if existing_company is not None else "planilha_padrao_folha_v1"
        ),
        active_config_id=existing_company.active_config_id if existing_company is not None else None,
        last_competence_seen=_max_competence(
            existing_company.last_competence_seen if existing_company is not None else None,
            normalized_competence,
        ),
        source_import="dashboard_manual",
        created_at=existing_company.created_at if existing_company is not None else now,
        updated_at=now,
    )
    company, _, _ = store.upsert_company(company)

    selected_config: CompanyConfigRecord | None = None
    if normalized_competence is not None:
        selected_config = _upsert_minimal_company_config(
            store,
            company=company,
            company_name=name,
            default_process=process,
            competence=normalized_competence,
            is_active=is_active,
            now=now,
        )
        if is_active and company.active_config_id != selected_config.id:
            company = company.model_copy(update={"active_config_id": selected_config.id, "updated_at": now})
            company, _, _ = store.upsert_company(company)

    return _admin_entry_from_registry(company, selected_config)


def save_employee_registry_record(
    *,
    company_code: str,
    domain_registration: str,
    employee_name: str,
    employee_key: str | None = None,
    aliases: str | list[str] | tuple[str, ...] | None = None,
    status: str = "active",
    notes: str | None = None,
    company_name: str | None = None,
    root: str | Path | None = None,
) -> Path:
    registry = load_company_employee_registry(
        _required_text("company_code", company_code),
        company_name=company_name,
        root=root,
    )
    employee = CompanyEmployeeRecord(
        employee_key=_optional_text(employee_key),
        employee_name=_required_text("employee_name", employee_name),
        domain_registration=_required_text("domain_registration", domain_registration),
        aliases=_aliases(aliases),
        status=status,
        source="dashboard_company_tab",
        notes=_optional_text(notes),
    )
    updated_registry = upsert_employee_record(registry, employee)
    return save_company_employee_registry(updated_registry, root=root)


def save_rubric_catalog_record(
    *,
    company_code: str,
    rubric_code: str,
    description: str,
    canonical_event: str | None,
    value_kind: str,
    nature: str,
    aliases: str | list[str] | tuple[str, ...] | None = None,
    status: str = "active",
    notes: str | None = None,
    company_name: str | None = None,
    root: str | Path | None = None,
) -> Path:
    catalog = load_company_rubric_catalog(
        _required_text("company_code", company_code),
        company_name=company_name,
        root=root,
    )
    rubric = CompanyRubricRecord(
        rubric_code=_required_text("rubric_code", rubric_code),
        description=_required_text("description", description),
        canonical_event=_optional_text(canonical_event) or _required_text("rubric_code", rubric_code),
        value_kind=_required_text("value_kind", value_kind),
        nature=_required_text("nature", nature),
        aliases=_aliases(aliases),
        status=status,
        source="dashboard_company_tab",
        notes=_optional_text(notes),
    )
    updated_catalog = upsert_rubric_record(catalog, rubric)
    return save_company_rubric_catalog(updated_catalog, root=root)


def save_column_mapping_profile_rule(
    *,
    company_code: str,
    column_name: str | None = None,
    value_kind: str,
    generation_mode: str,
    rubrica_target: str | None = None,
    rubricas_target: str | list[str] | tuple[str, ...] | None = None,
    sheet_name: str | None = None,
    header_row: int | str | None = None,
    data_start_row: int | str | None = None,
    employee_code_column: str | None = None,
    employee_name_column: str | None = None,
    row_control_column: str | None = None,
    ignore_row_when_contains: str | list[str] | tuple[str, ...] | None = None,
    stop_reading_when_contains: str | list[str] | tuple[str, ...] | None = None,
    value_column: str | None = None,
    expected_header: str | None = None,
    nature: str | None = None,
    status: str = "active",
    ignore_zero: bool = True,
    ignore_text: bool = True,
    enabled: bool | None = None,
    notes: str | None = None,
    company_name: str | None = None,
    default_process: str | None = None,
    root: str | Path | None = None,
) -> Path:
    code = _required_text("company_code", company_code)
    mode = ColumnGenerationMode(_required_text("generation_mode", generation_mode))
    column = _optional_text(column_name)
    event_column = _optional_text(value_column)
    if not column and not event_column:
        raise ValueError("column_name or value_column is required.")

    if mode == ColumnGenerationMode.IGNORE:
        if _optional_text(rubrica_target) or _target_list(rubricas_target):
            raise ValueError("ignore mappings cannot define rubrica targets.")
        rule_enabled = False
        single_target = None
        multiple_targets: list[str] = []
    elif mode == ColumnGenerationMode.SINGLE_LINE:
        rule_enabled = True if enabled is None else bool(enabled)
        single_target = _required_text("rubrica_target", rubrica_target)
        multiple_targets = []
    elif mode == ColumnGenerationMode.MULTI_LINE:
        rule_enabled = True if enabled is None else bool(enabled)
        single_target = None
        multiple_targets = _target_list(rubricas_target)
        if len(multiple_targets) < 2:
            raise ValueError("multi_line mappings require at least two rubricas.")
    else:  # pragma: no cover - enum exhaustiveness guard
        raise ValueError(f"Unsupported generation mode: {generation_mode}.")

    rule = ColumnMappingRule(
        column_name=column,
        sheet_name=_optional_text(sheet_name),
        header_row=_optional_int(header_row),
        data_start_row=_optional_int(data_start_row),
        employee_code_column=_optional_text(employee_code_column),
        employee_name_column=_optional_text(employee_name_column),
        row_control_column=_optional_text(row_control_column),
        ignore_row_tokens=_control_tokens(ignore_row_when_contains),
        stop_row_tokens=_control_tokens(stop_reading_when_contains),
        value_column=event_column,
        expected_header=_optional_text(expected_header),
        enabled=rule_enabled,
        rubrica_target=single_target,
        rubricas_target=multiple_targets,
        value_kind=_required_text("value_kind", value_kind),
        nature=_optional_text(nature) or "unknown",
        generation_mode=mode,
        ignore_zero=bool(ignore_zero),
        ignore_text=bool(ignore_text),
        status=_required_text("status", status),
        notes=_optional_text(notes),
    )

    try:
        profile = load_column_mapping_profile(code, root=root)
        updated_profile = upsert_column_mapping_rule(profile, rule)
    except ColumnMappingProfileError as exc:
        if exc.code != "profile_not_found":
            raise
        updated_profile = CompanyColumnMappingProfile(
            company_code=code,
            company_name=_optional_text(company_name),
            default_process=_optional_text(default_process),
            mappings=[rule],
        )

    metadata_updates: dict[str, str] = {}
    if _optional_text(company_name) and not updated_profile.company_name:
        metadata_updates["company_name"] = _required_text("company_name", company_name)
    if _optional_text(default_process) and not updated_profile.default_process:
        metadata_updates["default_process"] = _required_text("default_process", default_process)
    if metadata_updates:
        updated_profile = updated_profile.model_copy(update=metadata_updates)

    return save_column_mapping_profile(updated_profile, root=root)


def _control_tokens(values: str | list[str] | tuple[str, ...] | None) -> list[str]:
    if values is None:
        return []
    if isinstance(values, str):
        raw_values = re.split(r"[;,]", values)
    else:
        raw_values = [str(value) for value in values]
    tokens = [value.strip() for value in raw_values if value and value.strip()]
    seen: set[str] = set()
    ordered: list[str] = []
    for token in tokens:
        normalized = token.casefold()
        if normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(token)
    return ordered


def _upsert_minimal_company_config(
    store: CompanyMasterDataStore,
    *,
    company: CompanyRegistryEntry,
    company_name: str,
    default_process: str,
    competence: str,
    is_active: bool,
    now: datetime,
) -> CompanyConfigRecord:
    existing = _select_config_for_competence(store.find_configs_for_company(company.id), competence)
    version = existing.version if existing is not None else f"dashboard-minimal-{competence.replace('/', '-')}"
    base_payload = dict(existing.config_payload_internal) if existing is not None else {}
    config = CompanyConfig.model_validate(
        {
            **base_payload,
            "company_code": company.company_code,
            "company_name": company_name,
            "default_process": default_process,
            "competence": competence,
            "config_version": version,
            "event_mappings": list(base_payload.get("event_mappings", [])),
            "employee_mappings": list(base_payload.get("employee_mappings", [])),
            "pending_policy": base_payload.get("pending_policy", {}),
            "validation_flags": dict(base_payload.get("validation_flags", {})),
            "notes": base_payload.get("notes"),
        }
    )
    record = CompanyConfigRecord(
        id=existing.id if existing is not None else company_config_id(company.id, version),
        company_id=company.id,
        version=version,
        competence_start=competence,
        competence_end=competence,
        status="active" if is_active else "inactive",
        config_payload_internal=config.model_dump(mode="json"),
        validated_at=now,
        created_at=existing.created_at if existing is not None else now,
        updated_at=now,
    )
    record, _, _ = store.upsert_company_config(record)
    return record


def _select_display_config_record(
    company: CompanyRegistryEntry,
    records: list[CompanyConfigRecord],
) -> CompanyConfigRecord | None:
    if company.active_config_id:
        for record in records:
            if record.id == company.active_config_id:
                return record
    if not records:
        return None
    return sorted(
        records,
        key=lambda record: (
            competence_sort_key(record.competence_end),
            competence_sort_key(record.competence_start),
            record.version,
        ),
        reverse=True,
    )[0]


def _select_config_for_competence(
    records: tuple[CompanyConfigRecord, ...],
    competence: str,
) -> CompanyConfigRecord | None:
    matching = [
        record
        for record in records
        if _competence_in_range(
            competence,
            start=record.competence_start,
            end=record.competence_end,
        )
    ]
    if matching:
        return sorted(matching, key=lambda record: record.version)[0]
    return None


def _competence_in_range(competence: str, *, start: str | None, end: str | None) -> bool:
    target = competence_sort_key(competence)
    start_key = competence_sort_key(start)
    end_key = competence_sort_key(end)
    if start_key != (-1, -1) and target < start_key:
        return False
    if end_key != (-1, -1) and target > end_key:
        return False
    return start_key != (-1, -1) or end_key != (-1, -1)


def _admin_entry_from_registry(
    company: CompanyRegistryEntry,
    record: CompanyConfigRecord | None,
) -> CompanyAdminEntry:
    payload = _valid_config_payload(record)
    return CompanyAdminEntry(
        company_code=company.company_code,
        company_name=(
            company.razao_social
            or company.nome_fantasia
            or str(payload.get("company_name") or "")
            or company.company_code
        ),
        status=company.status,
        is_active=company.is_active,
        company_id=company.id,
        active_config_id=company.active_config_id,
        default_process=_optional_text(payload.get("default_process")),
        competence=_optional_text(payload.get("competence") or (record.competence_end if record else None)),
        config_version=record.version if record is not None else None,
        config_status=record.status if record is not None else None,
    )


def _admin_entry_from_config_record(record: CompanyConfigRecord) -> CompanyAdminEntry | None:
    payload = _valid_config_payload(record)
    code = _optional_text(payload.get("company_code"))
    if code is None:
        return None
    return CompanyAdminEntry(
        company_code=code,
        company_name=_optional_text(payload.get("company_name")) or code,
        status=record.status,
        is_active=record.status in {"active", "validated", "approved"},
        company_id=record.company_id,
        active_config_id=record.id,
        default_process=_optional_text(payload.get("default_process")),
        competence=_optional_text(payload.get("competence") or record.competence_end),
        config_version=record.version,
        config_status=record.status,
    )


def _valid_config_payload(record: CompanyConfigRecord | None) -> dict[str, Any]:
    if record is None:
        return {}
    try:
        return CompanyConfig.model_validate(record.config_payload_internal).model_dump(mode="json")
    except (ValidationError, ValueError):
        return {}


def _company_entry_sort_key(entry: CompanyAdminEntry) -> tuple[int, str]:
    digits = re.sub(r"\D+", "", entry.company_code)
    return (int(digits) if digits else 10**12, entry.company_code)


def _aliases(value: str | list[str] | tuple[str, ...] | None) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        raw_items = value.split(",")
    else:
        raw_items = list(value)
    return [item for item in (_optional_text(raw) for raw in raw_items) if item]


def _target_list(value: str | list[str] | tuple[str, ...] | None) -> list[str]:
    return _aliases(value)


def _required_text(field_name: str, value: Any) -> str:
    text = _optional_text(value)
    if text is None:
        raise ValueError(f"{field_name} is required.")
    return text


def _optional_text(value: Any) -> str | None:
    text = normalize_text(value)
    return text if text else None


def _optional_int(value: Any) -> int | None:
    text = _optional_text(value)
    if text is None:
        return None
    try:
        return int(text)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"integer value expected: {value}.") from exc


def _optional_competence(value: str | None) -> str | None:
    text = _optional_text(value)
    if text is None:
        return None
    normalized = normalize_competence(text)
    if normalized is None:
        return None
    return normalized


def _max_competence(left: str | None, right: str | None) -> str | None:
    if left is None:
        return right
    if right is None:
        return left
    return right if competence_sort_key(right) >= competence_sort_key(left) else left


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


__all__ = [
    "CompanyAdminEntry",
    "get_company_admin_entry",
    "list_company_admin_entries",
    "save_column_mapping_profile_rule",
    "save_company_admin_entry",
    "save_employee_registry_record",
    "save_rubric_catalog_record",
]
