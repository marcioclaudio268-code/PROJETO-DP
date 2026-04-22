from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from openpyxl import Workbook

from config import (
    CompanyConfig,
    CompanyConfigRecord,
    CompanyMasterDataStore,
    CompanyRegistryEntry,
    import_resumo_mensal_file,
)
from dashboard import ConfigResolutionStatus, ConfigResolver


def _write_summary_workbook(path: Path, rows: list[dict[str, object]]) -> Path:
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Resumo Mensal"
    headers = [
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
    ]
    sheet.append(headers)
    for row in rows:
        sheet.append([row.get(header) for header in headers])
    workbook.save(path)
    return path


def _write_legacy_config(root: Path, *, company_code: str, file_name: str, competence: str, config_version: str) -> Path:
    payload = {
        "company_code": company_code,
        "company_name": "Dela More",
        "default_process": "11",
        "competence": competence,
        "config_version": config_version,
        "event_mappings": [
            {"event_negocio": "gratificacao", "rubrica_saida": "201"},
            {"event_negocio": "horas_extras_50", "rubrica_saida": "350"},
        ],
        "employee_mappings": [
            {"source_employee_key": "col-001", "source_employee_name": "Ana Lima", "domain_registration": "123"},
        ],
        "pending_policy": {
            "review_required_event_negocios": [],
            "review_required_fields": [],
            "block_on_ambiguous_observations": True,
            "block_on_unmapped_employee": True,
            "block_on_unmapped_event": True,
        },
        "validation_flags": {},
    }
    path = root / company_code / file_name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _specific_config_payload(*, company_code: str, competence: str, version: str) -> dict[str, object]:
    return {
        "company_code": company_code,
        "company_name": "Dela More",
        "default_process": "11",
        "competence": competence,
        "config_version": version,
        "event_mappings": [
            {"event_negocio": "gratificacao", "rubrica_saida": "201"},
        ],
        "employee_mappings": [
            {"source_employee_key": "col-001", "source_employee_name": "Ana Lima", "domain_registration": "123"},
        ],
        "pending_policy": {
            "review_required_event_negocios": [],
            "review_required_fields": [],
            "block_on_ambiguous_observations": True,
            "block_on_unmapped_employee": True,
            "block_on_unmapped_event": True,
        },
        "validation_flags": {},
    }


def test_import_resumo_mensal_upserts_company_and_links_legacy_config(tmp_path: Path) -> None:
    workbook_path = _write_summary_workbook(
        tmp_path / "Resumo Mensal.xlsx",
        rows=[
            {
                "company_code": "72",
                "cnpj": "12.345.678/0001-90",
                "razao_social": "Dela More Ltda",
                "nome_fantasia": "Dela More",
                "status": "Ativo",
                "is_active": "sim",
                "default_template_id": "planilha_padrao_folha_v1",
                "last_competence_seen": "03/2024",
                "config_version": "cfg-v1",
            },
            {
                "company_code": "72",
                "cnpj": "12.345.678/0001-90",
                "razao_social": "Dela More Ltda",
                "nome_fantasia": "Dela More",
                "status": "Ativo",
                "is_active": "sim",
                "default_template_id": "planilha_padrao_folha_v1",
                "last_competence_seen": "04/2024",
                "config_version": "cfg-v1",
            },
        ],
    )
    legacy_root = tmp_path / "legacy"
    _write_legacy_config(legacy_root, company_code="72", file_name="active.json", competence="03/2024", config_version="cfg-v1")

    result = import_resumo_mensal_file(
        workbook_path,
        store_root=tmp_path / "master",
        legacy_configs_root=legacy_root,
    )

    store = CompanyMasterDataStore(tmp_path / "master")
    companies = store.load_registry_entries()
    configs = store.load_config_records()

    assert result.rows_read == 2
    assert result.companies_created == 1
    assert result.companies_updated == 0
    assert len(companies) == 1
    assert companies[0].company_code == "72"
    assert companies[0].source_import == "resumo_mensal"
    assert companies[0].last_competence_seen == "04/2024"
    assert companies[0].active_config_id == "config:company:72:cfg-v1"
    assert len(configs) == 1
    assert configs[0].company_id == companies[0].id
    assert configs[0].version == "cfg-v1"


def test_import_resumo_mensal_records_issue_when_company_code_is_missing(tmp_path: Path) -> None:
    workbook_path = _write_summary_workbook(
        tmp_path / "Resumo Mensal.xlsx",
        rows=[
            {
                "cnpj": "12.345.678/0001-90",
                "razao_social": "Empresa Sem Codigo",
                "nome_fantasia": "Sem Codigo",
                "status": "Ativo",
                "is_active": "sim",
                "last_competence_seen": "03/2024",
            }
        ],
    )

    result = import_resumo_mensal_file(
        workbook_path,
        store_root=tmp_path / "master",
        legacy_configs_root=tmp_path / "legacy",
    )

    store = CompanyMasterDataStore(tmp_path / "master")
    issues = store.load_issues()

    assert result.issues_created == 1
    assert len(issues) == 1
    assert issues[0].issue_type == "company_identity_missing"


def test_config_resolver_prefers_registry_specific_config(tmp_path: Path) -> None:
    master_root = tmp_path / "master"
    store = CompanyMasterDataStore(master_root)
    company = CompanyRegistryEntry(
        id="company:72",
        company_code="72",
        cnpj="12345678000190",
        razao_social="Dela More",
        nome_fantasia="Dela More",
        status="active",
        is_active=True,
        default_template_id="planilha_padrao_folha_v1",
        active_config_id=None,
        last_competence_seen="03/2024",
        source_import="resumo_mensal",
        created_at=datetime(2024, 3, 1, tzinfo=timezone.utc),
        updated_at=datetime(2024, 3, 1, tzinfo=timezone.utc),
    )
    specific_record = CompanyConfigRecord(
        id="config:company:72:cfg-specific",
        company_id="company:72",
        version="cfg-specific",
        competence_start="03/2024",
        competence_end="03/2024",
        status="active",
        config_payload_internal=CompanyConfig.model_validate(_specific_config_payload(company_code="72", competence="03/2024", version="cfg-specific")).model_dump(mode="json"),
        validated_at=datetime(2024, 3, 1, tzinfo=timezone.utc),
        created_at=datetime(2024, 3, 1, tzinfo=timezone.utc),
        updated_at=datetime(2024, 3, 1, tzinfo=timezone.utc),
    )
    store.save_all(registry_entries=[company], config_records=[specific_record], issues=[])

    result = ConfigResolver(registry_root=master_root, legacy_root=tmp_path / "legacy").resolve(
        company_code="72",
        competence="03/2024",
    )

    assert result.status == ConfigResolutionStatus.FOUND
    assert result.config_source == "registry_company_competence"
    assert result.config_version == "cfg-specific"


def test_config_resolver_uses_registry_active_config_when_specific_missing(tmp_path: Path) -> None:
    master_root = tmp_path / "master"
    store = CompanyMasterDataStore(master_root)
    company = CompanyRegistryEntry(
        id="company:72",
        company_code="72",
        cnpj="12345678000190",
        razao_social="Dela More",
        nome_fantasia="Dela More",
        status="active",
        is_active=True,
        default_template_id="planilha_padrao_folha_v1",
        active_config_id="config:company:72:cfg-active",
        last_competence_seen="03/2024",
        source_import="resumo_mensal",
        created_at=datetime(2024, 3, 1, tzinfo=timezone.utc),
        updated_at=datetime(2024, 3, 1, tzinfo=timezone.utc),
    )
    active_record = CompanyConfigRecord(
        id="config:company:72:cfg-active",
        company_id="company:72",
        version="cfg-active",
        competence_start="01/2024",
        competence_end="02/2024",
        status="active",
        config_payload_internal=CompanyConfig.model_validate(
            _specific_config_payload(company_code="72", competence="01/2024", version="cfg-active")
        ).model_dump(mode="json"),
        validated_at=datetime(2024, 3, 1, tzinfo=timezone.utc),
        created_at=datetime(2024, 3, 1, tzinfo=timezone.utc),
        updated_at=datetime(2024, 3, 1, tzinfo=timezone.utc),
    )
    store.save_all(registry_entries=[company], config_records=[active_record], issues=[])

    result = ConfigResolver(registry_root=master_root, legacy_root=tmp_path / "legacy").resolve(
        company_code="72",
        competence="03/2024",
    )

    assert result.status == ConfigResolutionStatus.FOUND
    assert result.config_source == "registry_company_active"
    assert result.config_version == "cfg-active"
    assert result.config_payload is not None
    assert result.config_payload["competence"] == "03/2024"
