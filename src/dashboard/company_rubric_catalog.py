"""Persistent per-company rubric catalog used by guided dashboard actions."""

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


DEFAULT_COMPANY_RUBRIC_CATALOGS_ROOT = Path("data/company_rubric_catalogs")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class CompanyRubricCatalogError(RuntimeError):
    """Raised when a persisted rubric catalog cannot be loaded or saved safely."""


class RubricValueKind(StrEnum):
    MONETARY = "monetario"
    HOURS = "horas"
    QUANTITY = "quantidade"


class RubricNature(StrEnum):
    PROVENTO = "provento"
    DESCONTO = "desconto"
    INFORMATIVO = "informativo"
    UNKNOWN = "unknown"


class RubricStatus(StrEnum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    UNKNOWN = "unknown"


class RubricCatalogBaseModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class CompanyRubricRecord(RubricCatalogBaseModel):
    rubric_code: str = Field(..., min_length=1)
    description: str = Field(..., min_length=1)
    canonical_event: str = Field(..., min_length=1)
    value_kind: RubricValueKind
    nature: RubricNature = RubricNature.UNKNOWN
    aliases: list[str] = Field(default_factory=list)
    status: RubricStatus = RubricStatus.ACTIVE
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


class CompanyRubricCatalog(RubricCatalogBaseModel):
    company_code: str = Field(..., min_length=1)
    company_name: str | None = None
    rubrics: list[CompanyRubricRecord] = Field(default_factory=list)

    @model_validator(mode="after")
    def _reject_duplicate_active_rubric_codes(self) -> "CompanyRubricCatalog":
        active_codes: dict[str, int] = {}
        duplicates: set[str] = set()
        for rubric in self.rubrics:
            if rubric.status != RubricStatus.ACTIVE:
                continue
            code = normalize_rubric_lookup_token(rubric.rubric_code)
            active_codes[code] = active_codes.get(code, 0) + 1
            if active_codes[code] > 1:
                duplicates.add(rubric.rubric_code)

        if duplicates:
            raise ValueError(
                "duplicate active rubric_code values: "
                + ", ".join(sorted(duplicates))
            )
        return self


@dataclass(frozen=True, slots=True)
class RubricCatalogApplyResult:
    mappings_added: int = 0
    mappings_updated: int = 0
    ambiguous_sources: tuple[str, ...] = ()
    unmatched_sources: tuple[str, ...] = ()
    catalog_path: str | None = None


def company_rubric_catalog_path(
    company_code: str,
    *,
    root: str | Path | None = None,
) -> Path:
    code = str(company_code).strip()
    if not code:
        raise CompanyRubricCatalogError("company_code is required to resolve rubric catalog path.")
    base = Path(root) if root is not None else DEFAULT_COMPANY_RUBRIC_CATALOGS_ROOT
    return base / f"{code}.json"


def load_company_rubric_catalog(
    company_code: str,
    *,
    company_name: str | None = None,
    root: str | Path | None = None,
) -> CompanyRubricCatalog:
    path = company_rubric_catalog_path(company_code, root=root)
    if not path.exists():
        return CompanyRubricCatalog(
            company_code=str(company_code),
            company_name=company_name,
            rubrics=[],
        )

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise CompanyRubricCatalogError(f"Invalid rubric catalog JSON: {path}") from exc

    catalog = CompanyRubricCatalog.model_validate(payload)
    if catalog.company_code != str(company_code):
        raise CompanyRubricCatalogError(
            f"Rubric catalog company mismatch. expected={company_code}; received={catalog.company_code}."
        )
    return catalog


def save_company_rubric_catalog(
    catalog: CompanyRubricCatalog,
    *,
    root: str | Path | None = None,
) -> Path:
    validated = CompanyRubricCatalog.model_validate(catalog.model_dump(mode="json"))
    path = company_rubric_catalog_path(validated.company_code, root=root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(validated.model_dump(mode="json"), ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return path


def upsert_rubric_record(
    catalog: CompanyRubricCatalog,
    rubric: CompanyRubricRecord,
) -> CompanyRubricCatalog:
    rubrics = list(catalog.rubrics)
    target_index = _rubric_upsert_index(rubrics, rubric)
    if target_index is None:
        rubrics.append(rubric)
    else:
        rubrics[target_index] = rubric

    return CompanyRubricCatalog(
        company_code=catalog.company_code,
        company_name=catalog.company_name,
        rubrics=rubrics,
    )


def list_active_rubrics(catalog: CompanyRubricCatalog) -> tuple[CompanyRubricRecord, ...]:
    return tuple(rubric for rubric in catalog.rubrics if rubric.status == RubricStatus.ACTIVE)


def find_rubric_by_code(
    catalog: CompanyRubricCatalog,
    rubric_code: str,
    *,
    active_only: bool = True,
) -> tuple[CompanyRubricRecord, ...]:
    token = normalize_rubric_lookup_token(rubric_code)
    if not token:
        return ()
    return tuple(
        rubric
        for rubric in _eligible_rubrics(catalog, active_only=active_only)
        if normalize_rubric_lookup_token(rubric.rubric_code) == token
    )


def find_rubric_by_description_or_alias(
    catalog: CompanyRubricCatalog,
    description: str,
    *,
    active_only: bool = True,
) -> tuple[CompanyRubricRecord, ...]:
    token = normalize_rubric_lookup_token(description)
    if not token:
        return ()
    matches: list[CompanyRubricRecord] = []
    for rubric in _eligible_rubrics(catalog, active_only=active_only):
        names = [rubric.description, *rubric.aliases]
        if any(normalize_rubric_lookup_token(name) == token for name in names):
            matches.append(rubric)
    return tuple(matches)


def find_rubric_by_canonical_event(
    catalog: CompanyRubricCatalog,
    canonical_event: str,
    *,
    active_only: bool = True,
) -> tuple[CompanyRubricRecord, ...]:
    token = normalize_rubric_lookup_token(canonical_event)
    if not token:
        return ()
    return tuple(
        rubric
        for rubric in _eligible_rubrics(catalog, active_only=active_only)
        if normalize_rubric_lookup_token(rubric.canonical_event) == token
    )


def find_rubric(
    catalog: CompanyRubricCatalog,
    *,
    rubric_code: str | None = None,
    description: str | None = None,
    canonical_event: str | None = None,
    active_only: bool = True,
) -> tuple[CompanyRubricRecord, ...]:
    matches: dict[tuple[str, str], CompanyRubricRecord] = {}
    for rubric in (
        *find_rubric_by_code(catalog, rubric_code or "", active_only=active_only),
        *find_rubric_by_description_or_alias(catalog, description or "", active_only=active_only),
        *find_rubric_by_canonical_event(catalog, canonical_event or "", active_only=active_only),
    ):
        matches[_rubric_signature(rubric)] = rubric
    return tuple(matches.values())


def apply_rubric_catalog_to_editable_config(
    config_path: str | Path,
    *,
    company_code: str,
    snapshot_payload: dict[str, Any],
    root: str | Path | None = None,
) -> RubricCatalogApplyResult:
    catalog_path = company_rubric_catalog_path(company_code, root=root)
    if not catalog_path.exists():
        return RubricCatalogApplyResult(catalog_path=str(catalog_path))

    catalog = load_company_rubric_catalog(company_code, root=root)
    config_path = Path(config_path)
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    result = apply_rubric_catalog_to_config_payload(
        payload,
        catalog=catalog,
        rubric_sources=rubric_sources_from_snapshot(snapshot_payload),
        catalog_path=str(catalog_path),
    )
    if result.mappings_added or result.mappings_updated:
        config = CompanyConfig.model_validate(payload)
        config_path.write_text(
            json.dumps(config.model_dump(mode="json"), ensure_ascii=True, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    return result


def apply_rubric_catalog_to_config_payload(
    config_payload: dict[str, Any],
    *,
    catalog: CompanyRubricCatalog,
    rubric_sources: Iterable[dict[str, Any]],
    catalog_path: str | None = None,
) -> RubricCatalogApplyResult:
    mappings = list(config_payload.get("event_mappings", []))
    active_events = {
        normalize_rubric_lookup_token(item.get("event_negocio"))
        for item in mappings
        if item.get("active", True)
    }

    added = 0
    ambiguous: list[str] = []
    unmatched: list[str] = []
    for source in rubric_sources:
        canonical_event = _stringify(source.get("canonical_event"))
        rubric_code = _stringify(source.get("rubric_code"))
        description = _stringify(source.get("description"))
        if not canonical_event:
            continue
        if normalize_rubric_lookup_token(canonical_event) in active_events:
            continue

        match, is_ambiguous = _safe_rubric_match(
            catalog,
            canonical_event=canonical_event,
            rubric_code=rubric_code,
            description=description,
        )
        source_label = canonical_event
        if is_ambiguous:
            ambiguous.append(source_label)
            continue
        if match is None:
            unmatched.append(source_label)
            continue

        mappings.append(
            {
                "event_negocio": canonical_event,
                "rubrica_saida": match.rubric_code,
                "active": True,
                "notes": "Preenchido a partir do catalogo persistente de rubricas.",
            }
        )
        active_events.add(normalize_rubric_lookup_token(canonical_event))
        added += 1

    config_payload["event_mappings"] = mappings
    return RubricCatalogApplyResult(
        mappings_added=added,
        mappings_updated=0,
        ambiguous_sources=tuple(ambiguous),
        unmatched_sources=tuple(unmatched),
        catalog_path=catalog_path,
    )


def rubric_sources_from_snapshot(snapshot_payload: dict[str, Any]) -> tuple[dict[str, Any], ...]:
    sources: dict[str, dict[str, Any]] = {}
    for movement in snapshot_payload.get("movements", ()):
        canonical_event = _stringify(movement.get("event_name"))
        if not canonical_event:
            continue
        source = movement.get("source", {})
        sources.setdefault(
            canonical_event,
            {
                "canonical_event": canonical_event,
                "rubric_code": _stringify(movement.get("informed_rubric")),
                "description": _stringify(source.get("column_name")),
            },
        )
    return tuple(sources.values())


def normalize_rubric_lookup_token(value: Any) -> str:
    text = unicodedata.normalize("NFKD", "" if value is None else str(value))
    text = "".join(character for character in text if not unicodedata.combining(character))
    return " ".join(text.strip().lower().split())


def _rubric_upsert_index(
    rubrics: list[CompanyRubricRecord],
    rubric: CompanyRubricRecord,
) -> int | None:
    code = normalize_rubric_lookup_token(rubric.rubric_code)
    for index, existing in enumerate(rubrics):
        if normalize_rubric_lookup_token(existing.rubric_code) == code:
            return index
    return None


def _safe_rubric_match(
    catalog: CompanyRubricCatalog,
    *,
    canonical_event: str | None,
    rubric_code: str | None,
    description: str | None,
) -> tuple[CompanyRubricRecord | None, bool]:
    candidates: dict[tuple[str, str], CompanyRubricRecord] = {}
    for rubric in find_rubric(
        catalog,
        rubric_code=rubric_code,
        description=description,
        canonical_event=canonical_event,
    ):
        candidates[_rubric_signature(rubric)] = rubric

    if len(candidates) == 1:
        return next(iter(candidates.values())), False
    if len(candidates) > 1:
        return None, True
    return None, False


def _rubric_signature(rubric: CompanyRubricRecord) -> tuple[str, str]:
    return (
        normalize_rubric_lookup_token(rubric.rubric_code),
        normalize_rubric_lookup_token(rubric.canonical_event),
    )


def _eligible_rubrics(
    catalog: CompanyRubricCatalog,
    *,
    active_only: bool,
) -> tuple[CompanyRubricRecord, ...]:
    if not active_only:
        return tuple(catalog.rubrics)
    return list_active_rubrics(catalog)


def _stringify(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)
