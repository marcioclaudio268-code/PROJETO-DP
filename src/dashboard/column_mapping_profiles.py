"""Company-specific column mapping profiles for uploaded payroll workbooks."""

from __future__ import annotations

import json
import re
from enum import StrEnum
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_COLUMN_MAPPING_PROFILES_ROOT = PROJECT_ROOT / "data" / "company_mapping_profiles"


class ColumnValueKind(StrEnum):
    MONETARY = "monetario"
    HOURS = "horas"
    QUANTITY = "quantidade"


class ColumnMappingNature(StrEnum):
    PROVENTO = "provento"
    DESCONTO = "desconto"
    INFORMATIVO = "informativo"
    UNKNOWN = "unknown"


class ColumnGenerationMode(StrEnum):
    SINGLE_LINE = "single_line"
    MULTI_LINE = "multi_line"
    IGNORE = "ignore"


class ColumnMappingStatus(StrEnum):
    ACTIVE = "active"
    INACTIVE = "inactive"


class ColumnMappingProfileError(ValueError):
    """Raised when a column mapping profile cannot be persisted or loaded safely."""

    def __init__(self, code: str, message: str, *, source: str | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.source = source


class _StrictProfileModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class ColumnMappingRule(_StrictProfileModel):
    """Rule that maps one source column into one or more outbound rubric targets."""

    column_key: str | None = Field(default=None, min_length=1)
    column_name: str | None = Field(default=None, min_length=1)
    sheet_name: str | None = Field(default=None, min_length=1)
    header_row: int | None = Field(default=None, ge=1)
    data_start_row: int | None = Field(default=None, ge=1)
    employee_code_column: str | None = Field(default=None, min_length=1)
    employee_name_column: str | None = Field(default=None, min_length=1)
    value_column: str | None = Field(default=None, min_length=1)
    expected_header: str | None = Field(default=None, min_length=1)
    enabled: bool
    rubrica_target: str | None = Field(default=None, min_length=1)
    rubricas_target: list[str] = Field(default_factory=list)
    value_kind: ColumnValueKind
    nature: ColumnMappingNature = ColumnMappingNature.UNKNOWN
    generation_mode: ColumnGenerationMode
    ignore_zero: bool
    ignore_text: bool
    status: ColumnMappingStatus = ColumnMappingStatus.ACTIVE
    notes: str | None = None

    @model_validator(mode="after")
    def _validate_rule_contract(self) -> "ColumnMappingRule":
        self.employee_code_column = normalize_excel_column(self.employee_code_column) if self.employee_code_column else None
        self.employee_name_column = normalize_excel_column(self.employee_name_column) if self.employee_name_column else None
        self.value_column = normalize_excel_column(self.value_column) if self.value_column else None

        if not self.column_key and not self.column_name and not self.value_column:
            raise ValueError("ColumnMappingRule requires column_key, column_name or value_column.")

        if self.value_column:
            if self.header_row is None:
                raise ValueError("position mappings require header_row.")
            if self.data_start_row is None:
                raise ValueError("position mappings require data_start_row.")
            if not self.employee_code_column:
                raise ValueError("position mappings require employee_code_column.")
            if self.data_start_row <= self.header_row:
                raise ValueError("data_start_row must be greater than header_row.")

        targets = [target for target in ([self.rubrica_target] if self.rubrica_target else []) + self.rubricas_target]
        duplicate_targets = _duplicates(targets)
        if duplicate_targets:
            raise ValueError(f"duplicate rubrica targets: {', '.join(duplicate_targets)}")

        if self.status == ColumnMappingStatus.INACTIVE:
            return self

        if self.generation_mode == ColumnGenerationMode.IGNORE:
            if self.enabled:
                raise ValueError("ignored mappings must have enabled=false.")
            if targets:
                raise ValueError("ignored mappings cannot define rubrica targets.")
            return self

        if not self.enabled:
            raise ValueError("disabled mappings must use generation_mode=ignore.")

        if self.generation_mode == ColumnGenerationMode.SINGLE_LINE:
            if self.rubrica_target is None or self.rubricas_target:
                raise ValueError("single_line mappings require rubrica_target and no rubricas_target.")
            return self

        if self.generation_mode == ColumnGenerationMode.MULTI_LINE:
            if self.rubrica_target is not None or len(self.rubricas_target) < 2:
                raise ValueError("multi_line mappings require at least two rubricas_target and no rubrica_target.")
            return self

        return self

    @property
    def source_column_id(self) -> str:
        return self.column_key or self.column_name or self.value_column or ""

    @property
    def target_rubrics(self) -> tuple[str, ...]:
        if self.rubrica_target is not None:
            return (self.rubrica_target,)
        return tuple(self.rubricas_target)

    @property
    def is_active(self) -> bool:
        return self.status == ColumnMappingStatus.ACTIVE


class CompanyColumnMappingProfile(_StrictProfileModel):
    """Saved conversion profile for one company workbook layout."""

    company_code: str = Field(..., min_length=1)
    company_name: str | None = None
    default_process: str | None = Field(default=None, min_length=1)
    mappings: list[ColumnMappingRule] = Field(..., min_length=1)
    profile_version: str = Field(default="column-mapping-v1", min_length=1)
    notes: str | None = None

    @model_validator(mode="after")
    def _check_duplicate_columns(self) -> "CompanyColumnMappingProfile":
        column_ids = [_mapping_duplicate_key(mapping) for mapping in self.mappings if mapping.is_active]
        duplicates = _duplicates(column_ids)
        if duplicates:
            raise ValueError(f"duplicate column mappings: {', '.join(duplicates)}")
        return self


def save_column_mapping_profile(
    profile: CompanyColumnMappingProfile,
    *,
    root: str | Path | None = None,
) -> Path:
    target_path = column_mapping_profile_path(profile.company_code, root=root)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path.write_text(
        json.dumps(profile.model_dump(mode="json"), ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return target_path


def load_column_mapping_profile(
    company_code: str,
    *,
    root: str | Path | None = None,
) -> CompanyColumnMappingProfile:
    profile_path = column_mapping_profile_path(company_code, root=root)
    try:
        return CompanyColumnMappingProfile.model_validate_json(profile_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ColumnMappingProfileError(
            "profile_not_found",
            f"Perfil de mapeamento de colunas nao encontrado para a empresa {company_code}.",
            source=str(profile_path),
        ) from exc


def upsert_column_mapping_rule(
    profile: CompanyColumnMappingProfile,
    rule: ColumnMappingRule,
) -> CompanyColumnMappingProfile:
    mappings = list(profile.mappings)
    target_id = _mapping_upsert_key(rule)
    updated = False
    for index, existing in enumerate(mappings):
        if _mapping_upsert_key(existing) == target_id:
            mappings[index] = rule
            updated = True
            break

    if not updated:
        mappings.append(rule)

    return CompanyColumnMappingProfile(
        company_code=profile.company_code,
        company_name=profile.company_name,
        default_process=profile.default_process,
        mappings=mappings,
        profile_version=profile.profile_version,
        notes=profile.notes,
    )


def column_mapping_profile_path(company_code: str, *, root: str | Path | None = None) -> Path:
    normalized_code = _safe_company_code(company_code)
    base = Path(root) if root is not None else DEFAULT_COLUMN_MAPPING_PROFILES_ROOT
    return base / f"{normalized_code}.json"


def _safe_company_code(company_code: str) -> str:
    text = str(company_code).strip()
    if not text or not re.fullmatch(r"[A-Za-z0-9_.-]+", text):
        raise ColumnMappingProfileError(
            "invalid_company_code",
            "Codigo de empresa invalido para caminho de perfil de mapeamento.",
            source=text or None,
        )
    return text


def _duplicates(values: list[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    duplicated: set[str] = set()
    for value in values:
        if value in seen:
            duplicated.add(value)
        seen.add(value)
    return tuple(sorted(duplicated))


def normalize_excel_column(value: str | None) -> str:
    text = str(value or "").strip().upper()
    if not re.fullmatch(r"[A-Z]+", text):
        raise ValueError(f"Invalid Excel column letter: {value}.")
    return text


def excel_column_to_index(value: str) -> int:
    column = normalize_excel_column(value)
    index = 0
    for character in column:
        index = index * 26 + (ord(character) - ord("A") + 1)
    return index


def _mapping_duplicate_key(mapping: ColumnMappingRule) -> str:
    sheet = _normalize_scope_token(mapping.sheet_name or "*")
    column = mapping.value_column or mapping.source_column_id
    return f"{sheet}:{_normalize_scope_token(column)}"


def _mapping_upsert_key(mapping: ColumnMappingRule) -> str:
    return _mapping_duplicate_key(mapping)


def _normalize_scope_token(value: str) -> str:
    return str(value).strip().upper()


__all__ = [
    "ColumnGenerationMode",
    "ColumnMappingNature",
    "ColumnMappingProfileError",
    "ColumnMappingRule",
    "ColumnMappingStatus",
    "ColumnValueKind",
    "CompanyColumnMappingProfile",
    "DEFAULT_COLUMN_MAPPING_PROFILES_ROOT",
    "column_mapping_profile_path",
    "excel_column_to_index",
    "load_column_mapping_profile",
    "normalize_excel_column",
    "save_column_mapping_profile",
    "upsert_column_mapping_rule",
]
