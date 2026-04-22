"""Internal master-data store and import helpers for company registry data."""

from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from pydantic import ValidationError

from .models import (
    CompanyConfig,
    CompanyConfigIssue,
    CompanyConfigRecord,
    CompanyRegistryEntry,
    MasterDataImportResult,
)


DEFAULT_MASTER_DATA_ROOT = Path(__file__).resolve().parents[2] / "data" / "company_master"
DEFAULT_LEGACY_COMPANY_CONFIGS_ROOT = Path(__file__).resolve().parents[2] / "configs" / "companies"

REGISTRY_FILENAME = "companies_registry.json"
CONFIGS_FILENAME = "company_configs.json"
ISSUES_FILENAME = "company_config_issues.json"
SUMMARY_IMPORT_SOURCE = "resumo_mensal"


class MasterDataImportError(RuntimeError):
    """Raised when the summary workbook cannot be parsed in a usable way."""


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True, slots=True)
class MasterDataPaths:
    root: Path
    registry_path: Path
    configs_path: Path
    issues_path: Path


@dataclass(frozen=True, slots=True)
class SummaryImportExtraction:
    sheet_name: str
    rows: list[dict[str, Any]]
    detected_fields: tuple[str, ...]
    sections_read: int
    companies_seen: int
    duplicate_company_sections_ignored: int
    parse_mode: str


def build_master_data_paths(root: str | Path | None = None) -> MasterDataPaths:
    base = Path(root) if root is not None else DEFAULT_MASTER_DATA_ROOT
    return MasterDataPaths(
        root=base,
        registry_path=base / REGISTRY_FILENAME,
        configs_path=base / CONFIGS_FILENAME,
        issues_path=base / ISSUES_FILENAME,
    )


def normalize_text(value: Any) -> str | None:
    if value is None:
        return None
    text = unicodedata.normalize("NFKC", str(value)).strip()
    if not text:
        return None
    return " ".join(text.split())


def normalize_digits(value: Any) -> str | None:
    text = normalize_text(value)
    if text is None:
        return None
    digits = re.sub(r"\D+", "", text)
    return digits or None


def normalize_cnpj(value: Any) -> str | None:
    digits = normalize_digits(value)
    if digits is None:
        return None
    return digits


def normalize_boolean(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = normalize_text(value)
    if text is None:
        return None
    token = text.lower()
    if token in {"1", "true", "sim", "s", "yes", "y", "ativo", "active"}:
        return True
    if token in {"0", "false", "nao", "n", "no", "inativo", "inactive"}:
        return False
    return None


def normalize_status(value: Any, *, default: str = "active") -> str:
    text = normalize_text(value)
    if text is None:
        return default
    token = text.lower()
    token = token.replace(" ", "_")
    token = token.replace("-", "_")
    return token


def normalize_competence(value: Any) -> str | None:
    if isinstance(value, datetime):
        return f"{value.month:02d}/{value.year:04d}"
    if isinstance(value, date):
        return f"{value.month:02d}/{value.year:04d}"
    text = normalize_text(value)
    if text is None:
        return None
    month_year = re.fullmatch(r"(?P<month>\d{1,2})[/-](?P<year>\d{4})", text)
    if month_year:
        month = int(month_year.group("month"))
        year = int(month_year.group("year"))
        if 1 <= month <= 12:
            return f"{month:02d}/{year:04d}"
    year_month = re.fullmatch(r"(?P<year>\d{4})[/-](?P<month>\d{1,2})", text)
    if year_month:
        month = int(year_month.group("month"))
        year = int(year_month.group("year"))
        if 1 <= month <= 12:
            return f"{month:02d}/{year:04d}"
    return text


def competence_sort_key(value: str | None) -> tuple[int, int]:
    normalized = normalize_competence(value)
    if not normalized:
        return -1, -1
    match = re.fullmatch(r"(?P<month>\d{2})/(?P<year>\d{4})", normalized)
    if not match:
        return -1, -1
    return int(match.group("year")), int(match.group("month"))


def company_registry_id(company_code: str | None, cnpj: str | None) -> str:
    if company_code:
        return f"company:{company_code}"
    if cnpj:
        return f"company:cnpj:{cnpj}"
    raise ValueError("company_code or cnpj is required to build the registry id")


def company_config_id(company_id: str, version: str) -> str:
    return f"config:{company_id}:{version}"


def issue_id(company_id: str, issue_type: str, description: str) -> str:
    digest = hashlib.sha1(description.encode("utf-8")).hexdigest()[:10]
    return f"issue:{company_id}:{issue_type}:{digest}"


def legacy_specific_config_candidates(company_code: str, competence: str, legacy_root: str | Path | None = None) -> tuple[Path, ...]:
    root = Path(legacy_root) if legacy_root is not None else DEFAULT_LEGACY_COMPANY_CONFIGS_ROOT
    company_root = root / company_code
    safe_dash = competence.replace("/", "-")
    safe_underscore = competence.replace("/", "_")
    candidates = [company_root / f"{safe_dash}.json"]
    if safe_underscore != safe_dash:
        candidates.append(company_root / f"{safe_underscore}.json")
    return tuple(candidates)


def legacy_active_config_path(company_code: str, legacy_root: str | Path | None = None) -> Path:
    root = Path(legacy_root) if legacy_root is not None else DEFAULT_LEGACY_COMPANY_CONFIGS_ROOT
    return root / company_code / "active.json"


def load_legacy_company_config_payload(
    company_code: str,
    competence: str,
    *,
    legacy_root: str | Path | None = None,
) -> tuple[CompanyConfig | None, Path | None]:
    candidates = tuple(path for path in legacy_specific_config_candidates(company_code, competence, legacy_root=legacy_root) if path.exists())
    if len(candidates) > 1:
        raise MasterDataImportError(
            f"Mais de uma configuracao legada candidata foi encontrada para a empresa {company_code} e competencia {competence}."
        )
    if len(candidates) == 1:
        return _load_company_config_from_path(candidates[0]), candidates[0]

    active_path = legacy_active_config_path(company_code, legacy_root=legacy_root)
    if active_path.exists():
        return _load_company_config_from_path(active_path), active_path

    return None, None


class CompanyMasterDataStore:
    """JSON-backed internal master data store for companies and configs."""

    def __init__(self, root: str | Path | None = None) -> None:
        self.paths = build_master_data_paths(root)

    def load_registry_entries(self) -> tuple[CompanyRegistryEntry, ...]:
        return tuple(CompanyRegistryEntry.model_validate(item) for item in _load_json_list(self.paths.registry_path))

    def load_config_records(self) -> tuple[CompanyConfigRecord, ...]:
        return tuple(CompanyConfigRecord.model_validate(item) for item in _load_json_list(self.paths.configs_path))

    def load_issues(self) -> tuple[CompanyConfigIssue, ...]:
        return tuple(CompanyConfigIssue.model_validate(item) for item in _load_json_list(self.paths.issues_path))

    def save_all(
        self,
        *,
        registry_entries: list[CompanyRegistryEntry],
        config_records: list[CompanyConfigRecord],
        issues: list[CompanyConfigIssue],
    ) -> None:
        _write_json_list(self.paths.registry_path, registry_entries)
        _write_json_list(self.paths.configs_path, config_records)
        _write_json_list(self.paths.issues_path, issues)

    def find_company_by_code(self, company_code: str) -> CompanyRegistryEntry | None:
        for item in self.load_registry_entries():
            if item.company_code == company_code:
                return item
        return None

    def find_company_by_cnpj(self, cnpj: str) -> CompanyRegistryEntry | None:
        for item in self.load_registry_entries():
            if item.cnpj == cnpj:
                return item
        return None

    def find_company_by_id(self, company_id: str) -> CompanyRegistryEntry | None:
        for item in self.load_registry_entries():
            if item.id == company_id:
                return item
        return None

    def find_config_by_id(self, config_id: str) -> CompanyConfigRecord | None:
        for item in self.load_config_records():
            if item.id == config_id:
                return item
        return None

    def find_configs_for_company(self, company_id: str) -> tuple[CompanyConfigRecord, ...]:
        return tuple(item for item in self.load_config_records() if item.company_id == company_id)

    def upsert_company(
        self,
        payload: CompanyRegistryEntry,
    ) -> tuple[CompanyRegistryEntry, bool, bool]:
        registry = list(self.load_registry_entries())
        created = True
        changed = True

        for index, item in enumerate(registry):
            if item.id == payload.id:
                created = False
                merged = _merge_registry_entry(item, payload)
                changed = merged.model_dump(mode="json") != item.model_dump(mode="json")
                registry[index] = merged
                self.save_all(
                    registry_entries=registry,
                    config_records=list(self.load_config_records()),
                    issues=list(self.load_issues()),
                )
                return merged, created, changed

        registry.append(payload)
        self.save_all(
            registry_entries=registry,
            config_records=list(self.load_config_records()),
            issues=list(self.load_issues()),
        )
        return payload, created, changed

    def upsert_company_config(
        self,
        payload: CompanyConfigRecord,
    ) -> tuple[CompanyConfigRecord, bool, bool]:
        configs = list(self.load_config_records())
        created = True
        changed = True

        for index, item in enumerate(configs):
            if item.id == payload.id:
                created = False
                changed = payload.model_dump(mode="json") != item.model_dump(mode="json")
                configs[index] = payload
                self.save_all(
                    registry_entries=list(self.load_registry_entries()),
                    config_records=configs,
                    issues=list(self.load_issues()),
                )
                return payload, created, changed

        configs.append(payload)
        self.save_all(
            registry_entries=list(self.load_registry_entries()),
            config_records=configs,
            issues=list(self.load_issues()),
        )
        return payload, created, changed

    def upsert_issue(self, payload: CompanyConfigIssue) -> tuple[CompanyConfigIssue, bool]:
        issues = list(self.load_issues())
        for index, item in enumerate(issues):
            if item.id == payload.id:
                issues[index] = payload
                self.save_all(
                    registry_entries=list(self.load_registry_entries()),
                    config_records=list(self.load_config_records()),
                    issues=issues,
                )
                return payload, False

        issues.append(payload)
        self.save_all(
            registry_entries=list(self.load_registry_entries()),
            config_records=list(self.load_config_records()),
            issues=issues,
        )
        return payload, True


def import_resumo_mensal_file(
    input_path: str | Path,
    *,
    store_root: str | Path | None = None,
    legacy_configs_root: str | Path | None = None,
) -> MasterDataImportResult:
    source_path = Path(input_path)
    if not source_path.exists():
        raise MasterDataImportError(f"Arquivo nao encontrado: {source_path}")

    extraction = _extract_summary_records(source_path)
    sheet_name = extraction.sheet_name
    store = CompanyMasterDataStore(store_root)

    companies_created = 0
    companies_updated = 0
    configs_created = 0
    configs_updated = 0
    issues_created = 0
    rows_read = extraction.sections_read
    duplicate_company_sections_ignored = 0

    seen_rows: dict[str, dict[str, Any]] = {}
    for row_index, normalized in enumerate(extraction.rows, start=1):
        company_code = normalized["company_code"]
        cnpj = normalized["cnpj"]
        if company_code is None:
            issue, created = store.upsert_issue(
                CompanyConfigIssue(
                    id=issue_id(
                        "unassigned",
                        "company_identity_missing",
                        f"linha_importada={row_index}",
                    ),
                    company_id="unassigned",
                    issue_type="company_identity_missing",
                    description=(
                        f"Linha importada {row_index} nao trouxe company_code nem cnpj; "
                        "nao foi possivel cadastrar a empresa."
                    ),
                )
            )
            issues_created += int(created)
            continue

        identity_key = _summary_identity_key(normalized)
        previous = seen_rows.get(identity_key)
        if previous is None:
            seen_rows[identity_key] = normalized
            continue

        merged, conflict_descriptions = _merge_summary_rows(previous, normalized)
        seen_rows[identity_key] = merged
        duplicate_company_sections_ignored += 1
        for description in conflict_descriptions:
            issue, created = store.upsert_issue(
                CompanyConfigIssue(
                    id=issue_id(identity_key, "summary_row_conflict", description),
                    company_id=identity_key,
                    issue_type="summary_row_conflict",
                    description=description,
                )
            )
            issues_created += int(created)

    companies_seen = len(seen_rows)

    for normalized in seen_rows.values():
        company_code = normalized["company_code"]
        cnpj = normalized["cnpj"]
        razao_social = normalized["razao_social"]
        nome_fantasia = normalized["nome_fantasia"]
        status = normalized["status"] or "active"
        is_active = normalized["is_active"]
        default_template_id = normalized["default_template_id"]
        active_config_id = normalized["active_config_id"]
        last_competence_seen = normalized["last_competence_seen"]
        competence_start = normalized["competence_start"]
        competence_end = normalized["competence_end"]

        registry_id = company_registry_id(company_code, cnpj)
        existing_by_code = store.find_company_by_code(company_code)
        existing_by_cnpj = store.find_company_by_cnpj(cnpj) if cnpj is not None else None

        if existing_by_code is not None and existing_by_cnpj is not None and existing_by_code.id != existing_by_cnpj.id:
            issue, created = store.upsert_issue(
                CompanyConfigIssue(
                    id=issue_id(registry_id, "registry_identity_conflict", f"company_code={company_code}|cnpj={cnpj}"),
                    company_id=registry_id,
                    issue_type="registry_identity_conflict",
                    description=(
                        f"Conflito entre company_code {company_code} e cnpj {cnpj} na importacao do resumo mensal."
                    ),
                )
            )
            issues_created += int(created)
            company = existing_by_code
        else:
            company = existing_by_code or existing_by_cnpj

        now = _utc_now()
        if company is None:
            company = CompanyRegistryEntry(
                id=registry_id,
                company_code=company_code,
                cnpj=cnpj,
                razao_social=razao_social,
                nome_fantasia=nome_fantasia,
                status=status,
                is_active=True if is_active is None else is_active,
                default_template_id=default_template_id,
                active_config_id=active_config_id,
                last_competence_seen=last_competence_seen,
                source_import=SUMMARY_IMPORT_SOURCE,
                created_at=now,
                updated_at=now,
            )
            company, created, changed = store.upsert_company(company)
            companies_created += int(created)
            companies_updated += int(changed and not created)
        else:
            merged = CompanyRegistryEntry(
                id=company.id,
                company_code=company.company_code or company_code,
                cnpj=company.cnpj or cnpj,
                razao_social=razao_social or company.razao_social,
                nome_fantasia=nome_fantasia or company.nome_fantasia,
                status=status or company.status,
                is_active=company.is_active if is_active is None else is_active,
                default_template_id=default_template_id or company.default_template_id,
                active_config_id=active_config_id or company.active_config_id,
                last_competence_seen=_max_competence(company.last_competence_seen, last_competence_seen),
                source_import=SUMMARY_IMPORT_SOURCE,
                created_at=company.created_at,
                updated_at=now,
            )
            company, created, changed = store.upsert_company(merged)
            companies_created += int(created)
            companies_updated += int(changed and not created)

        if last_competence_seen is not None:
            company, _, _ = store.upsert_company(
                company.model_copy(
                    update={
                        "last_competence_seen": _max_competence(company.last_competence_seen, last_competence_seen),
                        "updated_at": now,
                    }
                )
            )

        linked_record = None
        if active_config_id:
            linked_record = store.find_config_by_id(active_config_id)

        if linked_record is None and company_code:
            legacy_config, legacy_path = load_legacy_company_config_payload(
                company_code,
                competence=last_competence_seen or "01/0001",
                legacy_root=legacy_configs_root,
            )
            if legacy_config is not None and legacy_path is not None:
                linked_record = _company_config_record_from_payload(
                    company_id=company.id,
                    payload=legacy_config.model_dump(mode="json"),
                    competence_start=competence_start,
                    competence_end=competence_end,
                    status="active",
                )
                existing_config = store.find_config_by_id(linked_record.id)
                if existing_config is None:
                    _, created, changed = store.upsert_company_config(linked_record)
                    configs_created += int(created)
                    configs_updated += int(changed and not created)
                else:
                    _, created, changed = store.upsert_company_config(linked_record.model_copy(update={"updated_at": now}))
                    configs_created += int(created)
                    configs_updated += int(changed and not created)

                if company.active_config_id != linked_record.id:
                    company = company.model_copy(update={"active_config_id": linked_record.id, "updated_at": now})
                    store.upsert_company(company)

        if linked_record is None:
            issue, created = store.upsert_issue(
                CompanyConfigIssue(
                    id=issue_id(company.id, "company_config_missing", f"empresa={company.company_code}"),
                    company_id=company.id,
                    issue_type="company_config_missing",
                    description=(
                        f"Nenhuma configuracao interna foi localizada para a empresa {company.company_code} "
                        f"na competencia {last_competence_seen or 'desconhecida'}."
                    ),
                )
            )
            issues_created += int(created)

    store.save_all(
        registry_entries=list(store.load_registry_entries()),
        config_records=list(store.load_config_records()),
        issues=list(store.load_issues()),
    )

    return MasterDataImportResult(
        source_path=str(source_path),
        sheet_name=sheet_name,
        rows_read=rows_read,
        sections_read=extraction.sections_read,
        companies_seen=companies_seen,
        duplicate_company_sections_ignored=duplicate_company_sections_ignored,
        companies_created=companies_created,
        companies_updated=companies_updated,
        configs_created=configs_created,
        configs_updated=configs_updated,
        issues_created=issues_created,
        detected_fields=list(extraction.detected_fields),
        parse_mode=extraction.parse_mode,
        registry_path=str(store.paths.registry_path),
        configs_path=str(store.paths.configs_path),
        issues_path=str(store.paths.issues_path),
        message="Importacao do resumo mensal concluida.",
    )


def resolve_registry_config_payload(
    *,
    company_code: str,
    competence: str,
    registry_root: str | Path | None = None,
) -> tuple[dict[str, Any] | None, str | None, Path | None, str | None]:
    store = CompanyMasterDataStore(registry_root)
    company = store.find_company_by_code(company_code)
    if company is None:
        return None, None, None, None

    records = [record for record in store.find_configs_for_company(company.id) if record.status in {"active", "validated", "approved"}]
    matching = [record for record in records if _config_matches_competence(record, competence)]

    if len(matching) > 1:
        return None, "AMBIGUOUS", store.paths.configs_path, "Mais de uma configuracao interna candidata foi encontrada no cadastro mestre."

    if len(matching) == 1:
        record = matching[0]
        payload = _validated_company_config_payload(record.config_payload_internal, company_code, competence)
        if payload is None:
            return None, "MISMATCH", store.paths.configs_path, "A configuracao do cadastro mestre esta invalida ou inconsistente."
        return payload, "registry_company_competence", store.paths.configs_path, "Configuracao do cadastro mestre especifica da empresa e competencia encontrada."

    if company.active_config_id:
        record = store.find_config_by_id(company.active_config_id)
        if record is None:
            return None, "MISMATCH", store.paths.configs_path, "A empresa possui active_config_id, mas a configuracao vinculada nao foi encontrada."
        if record.company_id != company.id:
            return None, "MISMATCH", store.paths.configs_path, "A configuracao ativa vinculada pertence a outra empresa."
        if record.status not in {"active", "validated", "approved"}:
            return None, "MISMATCH", store.paths.configs_path, "A configuracao ativa vinculada nao esta habilitada."
        payload = _validated_company_config_payload(record.config_payload_internal, company_code, competence)
        if payload is None:
            return None, "MISMATCH", store.paths.configs_path, "A configuracao ativa vinculada esta invalida ou inconsistente."
        return payload, "registry_company_active", store.paths.configs_path, "Configuracao ativa do cadastro mestre aplicada como fallback."

    if records:
        return None, None, None, "A empresa existe no cadastro mestre, mas nao ha configuracao ativa vinculada para a competencia detectada."

    return None, None, None, None


def _read_spreadsheet(path: Path) -> dict[str, pd.DataFrame]:
    suffix = path.suffix.lower()
    if suffix == ".xlsx":
        return pd.read_excel(path, sheet_name=None, engine="openpyxl", dtype=object)
    if suffix == ".xls":
        try:
            return pd.read_excel(path, sheet_name=None, engine="calamine", dtype=object)
        except ImportError:
            pass
        try:
            return pd.read_excel(path, sheet_name=None, engine="xlrd", dtype=object)
        except ImportError as exc:  # pragma: no cover - depends on local installation
            raise MasterDataImportError(
                "A importacao de arquivos .xls requer a dependencia calamine ou xlrd. Instale as dependencias do projeto e tente novamente."
            ) from exc
    raise MasterDataImportError(f"Formato nao suportado: {path.suffix}. Use .xls ou .xlsx.")


def _read_spreadsheet_raw(path: Path) -> dict[str, pd.DataFrame]:
    suffix = path.suffix.lower()
    if suffix == ".xlsx":
        return pd.read_excel(path, sheet_name=None, engine="openpyxl", dtype=object, header=None)
    if suffix == ".xls":
        try:
            return pd.read_excel(path, sheet_name=None, engine="calamine", dtype=object, header=None)
        except ImportError:
            pass
        return pd.read_excel(path, sheet_name=None, engine="xlrd", dtype=object, header=None)
    raise MasterDataImportError(f"Formato nao suportado: {path.suffix}. Use .xls ou .xlsx.")


def _select_sheet(frames: dict[str, pd.DataFrame]) -> tuple[str, pd.DataFrame]:
    scored: list[tuple[int, str, pd.DataFrame]] = []
    for name, frame in frames.items():
        score = _sheet_score(frame.columns)
        if score > 0:
            scored.append((score, name, frame))
    if not scored:
        raise MasterDataImportError("Nao foi possivel identificar uma aba com colunas da planilha Resumo Mensal.")
    scored.sort(key=lambda item: (-item[0], item[1].lower()))
    top_score = scored[0][0]
    tied = [item for item in scored if item[0] == top_score]
    if len(tied) > 1:
        # The importer remains deterministic even when multiple candidate sheets tie.
        tied.sort(key=lambda item: item[1].lower())
    return tied[0][1], tied[0][2]


SUMMARY_ROW_ALIASES: dict[str, tuple[str, ...]] = {
    "company_code": ("company_code", "codigo_empresa", "codigo", "cod_empresa", "empresa_codigo", "filial", "cod_filial"),
    "cnpj": ("cnpj",),
    "razao_social": ("razao_social", "razao social", "empresa", "nome_empresa"),
    "nome_fantasia": ("nome_fantasia", "nome fantasia", "fantasia"),
    "status": ("status", "situacao"),
    "is_active": ("is_active", "ativo", "ativa", "status_ativo"),
    "default_template_id": ("default_template_id", "template", "modelo"),
    "active_config_id": ("active_config_id", "config_ativa", "config_atual"),
    "last_competence_seen": ("last_competence_seen", "competencia", "competencia_ultimo", "ultima_competencia"),
    "config_version": ("config_version", "versao_config", "version"),
    "competence_start": ("competence_start", "competencia_inicio", "competencia_inicial"),
    "competence_end": ("competence_end", "competencia_fim", "competencia_final"),
}


def _sheet_score(columns: pd.Index) -> int:
    normalized_columns = {_normalize_header(column) for column in columns}
    score = 0
    for aliases in SUMMARY_ROW_ALIASES.values():
        if any(alias in normalized_columns for alias in aliases):
            score += 1
    return score


def _normalize_summary_row(row: dict[str, Any]) -> dict[str, Any]:
    normalized_row = {_normalize_header(key): value for key, value in row.items()}

    result = {
        "company_code": _get_first(normalized_row, SUMMARY_ROW_ALIASES["company_code"]),
        "cnpj": normalize_cnpj(_get_first(normalized_row, SUMMARY_ROW_ALIASES["cnpj"])),
        "razao_social": normalize_text(_get_first(normalized_row, SUMMARY_ROW_ALIASES["razao_social"])),
        "nome_fantasia": normalize_text(_get_first(normalized_row, SUMMARY_ROW_ALIASES["nome_fantasia"])),
        "status": normalize_status(_get_first(normalized_row, SUMMARY_ROW_ALIASES["status"])),
        "is_active": normalize_boolean(_get_first(normalized_row, SUMMARY_ROW_ALIASES["is_active"])),
        "default_template_id": normalize_text(_get_first(normalized_row, SUMMARY_ROW_ALIASES["default_template_id"])),
        "active_config_id": normalize_text(_get_first(normalized_row, SUMMARY_ROW_ALIASES["active_config_id"])),
        "last_competence_seen": normalize_competence(_get_first(normalized_row, SUMMARY_ROW_ALIASES["last_competence_seen"])),
        "config_version": normalize_text(_get_first(normalized_row, SUMMARY_ROW_ALIASES["config_version"])),
        "competence_start": normalize_competence(_get_first(normalized_row, SUMMARY_ROW_ALIASES["competence_start"])),
        "competence_end": normalize_competence(_get_first(normalized_row, SUMMARY_ROW_ALIASES["competence_end"])),
    }
    company_code = normalize_text(result["company_code"])
    result["company_code"] = company_code
    result["is_active"] = True if result["is_active"] is None else result["is_active"]
    return result


def _extract_summary_records(source_path: Path) -> SummaryImportExtraction:
    try:
        frames = _read_spreadsheet(source_path)
        sheet_name, frame = _select_sheet(frames)
        records, detected_fields = _extract_tabular_summary_records(frame)
        if records:
            companies_seen = len({_summary_identity_key(record) for record in records})
            duplicate_sections_ignored = max(0, len(records) - companies_seen)
            return SummaryImportExtraction(
                sheet_name=sheet_name,
                rows=records,
                detected_fields=detected_fields,
                sections_read=len(records),
                companies_seen=companies_seen,
                duplicate_company_sections_ignored=duplicate_sections_ignored,
                parse_mode="tabular",
            )
    except MasterDataImportError:
        pass

    raw_frames = _read_spreadsheet_raw(source_path)
    sheet_name, frame = next(iter(raw_frames.items()))
    records, detected_fields = _extract_report_summary_records(frame)
    companies_seen = len({_summary_identity_key(record) for record in records})
    duplicate_sections_ignored = max(0, len(records) - companies_seen)
    return SummaryImportExtraction(
        sheet_name=sheet_name,
        rows=records,
        detected_fields=detected_fields,
        sections_read=len(records),
        companies_seen=companies_seen,
        duplicate_company_sections_ignored=duplicate_sections_ignored,
        parse_mode="report_blocks",
    )


def _extract_tabular_summary_records(frame: pd.DataFrame) -> tuple[list[dict[str, Any]], tuple[str, ...]]:
    records: list[dict[str, Any]] = []
    normalized_columns = {_normalize_header(column) for column in frame.columns}
    for row in frame.fillna("").to_dict(orient="records"):
        normalized = _normalize_summary_row(row)
        if normalized["company_code"] is None and normalized["cnpj"] is None:
            continue
        records.append(normalized)

    detected_fields = tuple(
        field
        for field in (
            "company_code",
            "cnpj",
            "razao_social",
            "nome_fantasia",
            "status",
            "is_active",
            "default_template_id",
            "active_config_id",
            "last_competence_seen",
            "config_version",
            "competence_start",
            "competence_end",
        )
        if any(_normalize_header(alias) in normalized_columns for alias in SUMMARY_ROW_ALIASES[field])
    )
    return records, detected_fields


def _extract_report_summary_records(frame: pd.DataFrame) -> tuple[list[dict[str, Any]], tuple[str, ...]]:
    records: list[dict[str, Any]] = []
    detected_fields = (
        "Empresa:",
        "CNPJ:",
        "Cálculo:",
        "Competência:",
        "Página:",
        "Emissão:",
        "Hora:",
    )

    for row_index in range(max(0, len(frame) - 3)):
        if (
            _normalize_header(frame.iat[row_index, 0]) == "empresa"
            and _normalize_header(frame.iat[row_index + 1, 0]) == "cnpj"
            and _normalize_header(frame.iat[row_index + 2, 0]) == "calculo"
            and _normalize_header(frame.iat[row_index + 3, 0]) == "competencia"
        ):
            records.append(_normalize_report_summary_row(frame, row_index))

    return records, detected_fields


def _normalize_report_summary_row(frame: pd.DataFrame, row_index: int) -> dict[str, Any]:
    company_text = normalize_text(frame.iat[row_index, 4]) or ""
    company_code, razao_social = _parse_company_header(company_text)
    cnpj = normalize_cnpj(frame.iat[row_index + 1, 4])
    competence = normalize_competence(frame.iat[row_index + 3, 4])
    calc_raw = normalize_text(frame.iat[row_index + 2, 4])
    is_active = True
    status = "active"
    if calc_raw and calc_raw.lower() != "folha mensal":
        status = "review"
        is_active = False

    return {
        "company_code": company_code,
        "cnpj": cnpj,
        "razao_social": razao_social,
        "nome_fantasia": razao_social,
        "status": status,
        "is_active": is_active,
        "default_template_id": "planilha_padrao_folha_v1",
        "active_config_id": None,
        "last_competence_seen": competence,
        "config_version": None,
        "competence_start": competence,
        "competence_end": competence,
    }


def _parse_company_header(value: str) -> tuple[str | None, str | None]:
    text = normalize_text(value)
    if text is None:
        return None, None
    match = re.match(r"^(?P<code>\d+)\s*-\s*(?P<name>.+)$", text)
    if match:
        return match.group("code"), match.group("name").strip()
    digits = normalize_digits(text)
    if digits:
        return digits, text
    return None, text


def _summary_identity_key(row: dict[str, Any]) -> str:
    company_code = normalize_text(row.get("company_code"))
    cnpj = normalize_cnpj(row.get("cnpj"))
    if company_code:
        return f"company:{company_code}"
    if cnpj:
        return f"company:cnpj:{cnpj}"
    return "company:unassigned"


def _merge_summary_rows(existing: dict[str, Any], incoming: dict[str, Any]) -> tuple[dict[str, Any], tuple[str, ...]]:
    merged = dict(existing)
    conflicts: list[str] = []

    for field in ("company_code", "cnpj", "razao_social", "nome_fantasia", "default_template_id", "active_config_id", "config_version"):
        current = merged.get(field)
        incoming_value = incoming.get(field)
        if current in {None, ""} and incoming_value not in {None, ""}:
            merged[field] = incoming_value
        elif current not in {None, ""} and incoming_value not in {None, ""} and current != incoming_value:
            conflicts.append(f"Campo {field} divergente entre blocos do resumo mensal: {current} x {incoming_value}.")

    for field in ("status", "is_active"):
        incoming_value = incoming.get(field)
        if incoming_value is not None:
            merged[field] = incoming_value

    merged["last_competence_seen"] = _max_competence(existing.get("last_competence_seen"), incoming.get("last_competence_seen"))
    merged["competence_start"] = _min_competence(existing.get("competence_start"), incoming.get("competence_start"))
    merged["competence_end"] = _max_competence(existing.get("competence_end"), incoming.get("competence_end"))
    return merged, tuple(conflicts)


def _min_competence(left: str | None, right: str | None) -> str | None:
    if left is None:
        return right
    if right is None:
        return left
    return left if competence_sort_key(left) <= competence_sort_key(right) else right


def _get_first(row: dict[str, Any], aliases: tuple[str, ...]) -> Any:
    for alias in aliases:
        if alias in row and row[alias] not in {"", None} and not _is_nan(row[alias]):
            return row[alias]
    return None


def _is_nan(value: Any) -> bool:
    try:
        return bool(pd.isna(value))
    except Exception:
        return False


def _merge_registry_entry(existing: CompanyRegistryEntry, incoming: CompanyRegistryEntry) -> CompanyRegistryEntry:
    return CompanyRegistryEntry(
        id=existing.id,
        company_code=incoming.company_code or existing.company_code,
        cnpj=incoming.cnpj or existing.cnpj,
        razao_social=incoming.razao_social or existing.razao_social,
        nome_fantasia=incoming.nome_fantasia or existing.nome_fantasia,
        status=incoming.status or existing.status,
        is_active=incoming.is_active,
        default_template_id=incoming.default_template_id or existing.default_template_id,
        active_config_id=incoming.active_config_id or existing.active_config_id,
        last_competence_seen=_max_competence(existing.last_competence_seen, incoming.last_competence_seen),
        source_import=incoming.source_import or existing.source_import,
        created_at=existing.created_at,
        updated_at=incoming.updated_at,
    )


def _company_config_record_from_payload(
    *,
    company_id: str,
    payload: dict[str, Any],
    competence_start: str | None,
    competence_end: str | None,
    status: str,
) -> CompanyConfigRecord:
    config = CompanyConfig.model_validate(payload)
    return CompanyConfigRecord(
        id=company_config_id(company_id, config.config_version),
        company_id=company_id,
        version=config.config_version,
        competence_start=competence_start or config.competence,
        competence_end=competence_end or config.competence,
        status=status,
        config_payload_internal=config.model_dump(mode="json"),
        validated_at=_utc_now(),
        created_at=_utc_now(),
        updated_at=_utc_now(),
    )


def _load_company_config_from_path(path: Path) -> CompanyConfig:
    raw_json = path.read_text(encoding="utf-8")
    return CompanyConfig.model_validate_json(raw_json)


def _validated_company_config_payload(
    payload: dict[str, Any],
    company_code: str,
    competence: str,
) -> dict[str, Any] | None:
    try:
        config = CompanyConfig.model_validate(payload)
    except ValidationError:
        return None
    if config.company_code != company_code:
        return None
    resolved = config.model_dump(mode="json")
    resolved["competence"] = competence
    return resolved


def _config_matches_competence(record: CompanyConfigRecord, competence: str) -> bool:
    target_key = competence_sort_key(competence)
    start_key = competence_sort_key(record.competence_start)
    end_key = competence_sort_key(record.competence_end)
    if start_key == (-1, -1) and end_key == (-1, -1):
        return False
    if start_key != (-1, -1) and target_key < start_key:
        return False
    if end_key != (-1, -1) and target_key > end_key:
        return False
    return True


def _max_competence(left: str | None, right: str | None) -> str | None:
    if left is None:
        return right
    if right is None:
        return left
    return right if competence_sort_key(right) >= competence_sort_key(left) else left


def _normalize_header(value: Any) -> str:
    text = normalize_text(value)
    if text is None:
        return ""
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return text.strip("_")


def _load_json_list(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json_list(path: Path, items: list[Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(
        [item.model_dump(mode="json") if hasattr(item, "model_dump") else item for item in items],
        ensure_ascii=True,
        indent=2,
        sort_keys=True,
    )
    path.write_text(payload + "\n", encoding="utf-8")
