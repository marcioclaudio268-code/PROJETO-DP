"""Persistent per-company employee registry used by guided dashboard actions."""

from __future__ import annotations

import json
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
from pathlib import Path
from typing import Any, Iterable

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from config import CompanyConfig


DEFAULT_COMPANY_EMPLOYEE_REGISTRIES_ROOT = Path("data/company_employee_registries")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class CompanyEmployeeRegistryError(RuntimeError):
    """Raised when a persisted employee registry cannot be loaded or saved safely."""


class EmployeeRegistryStatus(StrEnum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    UNKNOWN = "unknown"


class EmployeeRegistryBaseModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class CompanyEmployeeRecord(EmployeeRegistryBaseModel):
    employee_key: str | None = None
    employee_name: str = Field(..., min_length=1)
    domain_registration: str = Field(..., min_length=1)
    aliases: list[str] = Field(default_factory=list)
    status: EmployeeRegistryStatus = EmployeeRegistryStatus.ACTIVE
    source: str = Field(default="manual", min_length=1)
    notes: str | None = None
    updated_at: datetime = Field(default_factory=_utc_now)

    @field_validator("aliases")
    @classmethod
    def _strip_aliases(cls, value: list[str]) -> list[str]:
        aliases: list[str] = []
        for alias in value:
            text = str(alias).strip()
            if text:
                aliases.append(text)
        return aliases


class CompanyEmployeeRegistry(EmployeeRegistryBaseModel):
    company_code: str = Field(..., min_length=1)
    company_name: str | None = None
    employees: list[CompanyEmployeeRecord] = Field(default_factory=list)

    @model_validator(mode="after")
    def _reject_duplicate_active_registrations(self) -> "CompanyEmployeeRegistry":
        active_registrations: dict[str, int] = {}
        duplicates: set[str] = set()
        for employee in self.employees:
            if employee.status != EmployeeRegistryStatus.ACTIVE:
                continue
            registration = normalize_employee_lookup_token(employee.domain_registration)
            active_registrations[registration] = active_registrations.get(registration, 0) + 1
            if active_registrations[registration] > 1:
                duplicates.add(employee.domain_registration)

        if duplicates:
            raise ValueError(
                "duplicate active domain_registration values: "
                + ", ".join(sorted(duplicates))
            )
        return self


@dataclass(frozen=True, slots=True)
class EmployeeRegistryApplyResult:
    mappings_added: int = 0
    mappings_updated: int = 0
    ambiguous_sources: tuple[str, ...] = ()
    unmatched_sources: tuple[str, ...] = ()
    registry_path: str | None = None


def company_employee_registry_path(
    company_code: str,
    *,
    root: str | Path | None = None,
) -> Path:
    code = str(company_code).strip()
    if not code:
        raise CompanyEmployeeRegistryError("company_code is required to resolve employee registry path.")
    base = Path(root) if root is not None else DEFAULT_COMPANY_EMPLOYEE_REGISTRIES_ROOT
    return base / f"{code}.json"


def load_company_employee_registry(
    company_code: str,
    *,
    company_name: str | None = None,
    root: str | Path | None = None,
) -> CompanyEmployeeRegistry:
    path = company_employee_registry_path(company_code, root=root)
    if not path.exists():
        return CompanyEmployeeRegistry(
            company_code=str(company_code),
            company_name=company_name,
            employees=[],
        )

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise CompanyEmployeeRegistryError(f"Invalid employee registry JSON: {path}") from exc

    registry = CompanyEmployeeRegistry.model_validate(payload)
    if registry.company_code != str(company_code):
        raise CompanyEmployeeRegistryError(
            f"Employee registry company mismatch. expected={company_code}; received={registry.company_code}."
        )
    return registry


def save_company_employee_registry(
    registry: CompanyEmployeeRegistry,
    *,
    root: str | Path | None = None,
) -> Path:
    validated = CompanyEmployeeRegistry.model_validate(registry.model_dump(mode="json"))
    path = company_employee_registry_path(validated.company_code, root=root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(validated.model_dump(mode="json"), ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return path


def upsert_employee_record(
    registry: CompanyEmployeeRegistry,
    employee: CompanyEmployeeRecord,
) -> CompanyEmployeeRegistry:
    employees = list(registry.employees)
    target_index = _employee_upsert_index(employees, employee)
    if target_index is None:
        employees.append(employee)
    else:
        employees[target_index] = employee

    return CompanyEmployeeRegistry(
        company_code=registry.company_code,
        company_name=registry.company_name,
        employees=employees,
    )


def associate_employee_alias(
    registry: CompanyEmployeeRegistry,
    *,
    domain_registration: str,
    alias: str,
) -> CompanyEmployeeRegistry:
    alias_text = str(alias).strip()
    if not alias_text:
        raise ValueError("alias is required")

    employees = list(registry.employees)
    matches = [
        (index, employee)
        for index, employee in enumerate(employees)
        if employee.status == EmployeeRegistryStatus.ACTIVE
        and normalize_employee_lookup_token(employee.domain_registration)
        == normalize_employee_lookup_token(domain_registration)
    ]
    if not matches:
        raise ValueError(f"active employee not found for domain_registration={domain_registration}")

    index, employee = matches[0]
    existing_tokens = {normalize_employee_lookup_token(item) for item in employee.aliases}
    if normalize_employee_lookup_token(alias_text) in existing_tokens:
        return registry

    updated_aliases = [*employee.aliases, alias_text]
    employees[index] = employee.model_copy(update={"aliases": updated_aliases, "updated_at": _utc_now()})
    return CompanyEmployeeRegistry(
        company_code=registry.company_code,
        company_name=registry.company_name,
        employees=employees,
    )


def list_active_employees(registry: CompanyEmployeeRegistry) -> tuple[CompanyEmployeeRecord, ...]:
    return tuple(
        employee for employee in registry.employees if employee.status == EmployeeRegistryStatus.ACTIVE
    )


def find_employee_by_key(
    registry: CompanyEmployeeRegistry,
    employee_key: str,
    *,
    active_only: bool = True,
) -> tuple[CompanyEmployeeRecord, ...]:
    token = normalize_employee_lookup_token(employee_key)
    if not token:
        return ()
    return tuple(
        employee
        for employee in _eligible_employees(registry, active_only=active_only)
        if employee.employee_key and normalize_employee_lookup_token(employee.employee_key) == token
    )


def find_employee_by_name_or_alias(
    registry: CompanyEmployeeRegistry,
    employee_name: str,
    *,
    active_only: bool = True,
) -> tuple[CompanyEmployeeRecord, ...]:
    token = normalize_employee_lookup_token(employee_name)
    if not token:
        return ()
    matches: list[CompanyEmployeeRecord] = []
    for employee in _eligible_employees(registry, active_only=active_only):
        names = [employee.employee_name, *employee.aliases]
        if any(normalize_employee_lookup_token(name) == token for name in names):
            matches.append(employee)
    return tuple(matches)


def find_employee_by_domain_registration(
    registry: CompanyEmployeeRegistry,
    domain_registration: str,
    *,
    active_only: bool = True,
) -> tuple[CompanyEmployeeRecord, ...]:
    token = normalize_employee_lookup_token(domain_registration)
    if not token:
        return ()
    return tuple(
        employee
        for employee in _eligible_employees(registry, active_only=active_only)
        if normalize_employee_lookup_token(employee.domain_registration) == token
    )


def find_employee(
    registry: CompanyEmployeeRegistry,
    *,
    employee_key: str | None = None,
    employee_name: str | None = None,
    domain_registration: str | None = None,
    active_only: bool = True,
) -> tuple[CompanyEmployeeRecord, ...]:
    matches: dict[tuple[str | None, str, str], CompanyEmployeeRecord] = {}
    for employee in (
        *find_employee_by_key(registry, employee_key or "", active_only=active_only),
        *find_employee_by_name_or_alias(registry, employee_name or "", active_only=active_only),
        *find_employee_by_domain_registration(registry, domain_registration or "", active_only=active_only),
    ):
        matches[_employee_signature(employee)] = employee
    return tuple(matches.values())


def apply_employee_registry_to_editable_config(
    config_path: str | Path,
    *,
    company_code: str,
    snapshot_payload: dict[str, Any],
    root: str | Path | None = None,
) -> EmployeeRegistryApplyResult:
    registry_path = company_employee_registry_path(company_code, root=root)
    if not registry_path.exists():
        return EmployeeRegistryApplyResult(registry_path=str(registry_path))

    registry = load_company_employee_registry(company_code, root=root)
    config_path = Path(config_path)
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    result = apply_employee_registry_to_config_payload(
        payload,
        registry=registry,
        employee_sources=employee_sources_from_snapshot(snapshot_payload),
        registry_path=str(registry_path),
    )
    if result.mappings_added or result.mappings_updated:
        config = CompanyConfig.model_validate(payload)
        config_path.write_text(
            json.dumps(config.model_dump(mode="json"), ensure_ascii=True, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    return result


def apply_employee_registry_to_config_payload(
    config_payload: dict[str, Any],
    *,
    registry: CompanyEmployeeRegistry,
    employee_sources: Iterable[dict[str, Any]],
    registry_path: str | None = None,
) -> EmployeeRegistryApplyResult:
    mappings = list(config_payload.get("employee_mappings", []))
    active_keys = {
        normalize_employee_lookup_token(item.get("source_employee_key"))
        for item in mappings
        if item.get("active", True)
    }

    added = 0
    ambiguous: list[str] = []
    unmatched: list[str] = []
    for source in employee_sources:
        employee_key = _stringify(source.get("employee_key"))
        employee_name = _stringify(source.get("employee_name"))
        domain_registration = _stringify(source.get("domain_registration"))
        if not employee_key:
            continue
        if normalize_employee_lookup_token(employee_key) in active_keys:
            continue

        match, is_ambiguous = _safe_registry_match(
            registry,
            employee_key=employee_key,
            employee_name=employee_name,
            domain_registration=domain_registration,
        )
        source_label = employee_name or employee_key
        if is_ambiguous:
            ambiguous.append(source_label)
            continue
        if match is None:
            unmatched.append(source_label)
            continue

        mappings.append(
            {
                "source_employee_key": employee_key,
                "source_employee_name": employee_name or match.employee_name,
                "domain_registration": match.domain_registration,
                "active": True,
                "aliases": [],
                "notes": "Preenchido a partir do cadastro persistente de funcionarios.",
            }
        )
        active_keys.add(normalize_employee_lookup_token(employee_key))
        added += 1

    config_payload["employee_mappings"] = mappings
    return EmployeeRegistryApplyResult(
        mappings_added=added,
        mappings_updated=0,
        ambiguous_sources=tuple(ambiguous),
        unmatched_sources=tuple(unmatched),
        registry_path=registry_path,
    )


def employee_sources_from_snapshot(snapshot_payload: dict[str, Any]) -> tuple[dict[str, Any], ...]:
    sources: dict[str, dict[str, Any]] = {}
    for section_name in ("employees", "movements"):
        for item in snapshot_payload.get(section_name, ()):
            employee_key = _stringify(item.get("employee_key"))
            if not employee_key:
                continue
            sources.setdefault(
                employee_key,
                {
                    "employee_key": employee_key,
                    "employee_name": _stringify(item.get("employee_name")),
                    "domain_registration": _stringify(item.get("domain_registration")),
                },
            )
    return tuple(sources.values())


def normalize_employee_lookup_token(value: Any) -> str:
    text = unicodedata.normalize("NFKD", "" if value is None else str(value))
    text = "".join(character for character in text if not unicodedata.combining(character))
    return " ".join(text.strip().lower().split())


def _employee_upsert_index(
    employees: list[CompanyEmployeeRecord],
    employee: CompanyEmployeeRecord,
) -> int | None:
    if employee.employee_key:
        key = normalize_employee_lookup_token(employee.employee_key)
        for index, existing in enumerate(employees):
            if existing.employee_key and normalize_employee_lookup_token(existing.employee_key) == key:
                return index

    registration = normalize_employee_lookup_token(employee.domain_registration)
    for index, existing in enumerate(employees):
        if normalize_employee_lookup_token(existing.domain_registration) == registration:
            return index
    return None


def _safe_registry_match(
    registry: CompanyEmployeeRegistry,
    *,
    employee_key: str | None,
    employee_name: str | None,
    domain_registration: str | None,
) -> tuple[CompanyEmployeeRecord | None, bool]:
    candidates: dict[tuple[str | None, str, str], CompanyEmployeeRecord] = {}
    for employee in find_employee(
        registry,
        employee_key=employee_key,
        employee_name=employee_name,
        domain_registration=domain_registration,
    ):
        candidates[_employee_signature(employee)] = employee

    if len(candidates) == 1:
        return next(iter(candidates.values())), False
    if len(candidates) > 1:
        return None, True
    return None, False


def _employee_signature(employee: CompanyEmployeeRecord) -> tuple[str | None, str, str]:
    return (
        employee.employee_key,
        normalize_employee_lookup_token(employee.employee_name),
        normalize_employee_lookup_token(employee.domain_registration),
    )


def _eligible_employees(
    registry: CompanyEmployeeRegistry,
    *,
    active_only: bool,
) -> tuple[CompanyEmployeeRecord, ...]:
    if not active_only:
        return tuple(registry.employees)
    return list_active_employees(registry)


def _stringify(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)
