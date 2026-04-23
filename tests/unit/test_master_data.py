from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from openpyxl import Workbook

from config import (
    CompanyConfig,
    CompanyConfigIssue,
    CompanyConfigRecord,
    CompanyMasterDataStore,
    CompanyRegistryEntry,
    import_resumo_mensal_file,
    seed_event_mappings_from_catalog,
    seed_company_configs_from_missing_issues,
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


def _seed_registry_entry(
    *,
    company_code: str,
    competence: str,
    cnpj: str,
    active_config_id: str | None = None,
) -> CompanyRegistryEntry:
    return CompanyRegistryEntry(
        id=f"company:{company_code}",
        company_code=company_code,
        cnpj=cnpj,
        razao_social=f"Empresa {company_code}",
        nome_fantasia=f"Empresa {company_code}",
        status="active",
        is_active=True,
        default_template_id="planilha_padrao_folha_v1",
        active_config_id=active_config_id,
        last_competence_seen=competence,
        source_import="resumo_mensal",
        created_at=datetime(2026, 4, 1, tzinfo=timezone.utc),
        updated_at=datetime(2026, 4, 1, tzinfo=timezone.utc),
    )


def _seed_missing_issue(*, company_code: str, competence: str) -> CompanyConfigIssue:
    return CompanyConfigIssue(
        id=f"issue:company:{company_code}:company_config_missing:{competence.replace('/', '-')}",
        company_id=f"company:{company_code}",
        issue_type="company_config_missing",
        description=f"Nenhuma configuracao interna foi localizada para a empresa {company_code} na competencia {competence}.",
        status="open",
        created_at=datetime(2026, 4, 1, tzinfo=timezone.utc),
        updated_at=datetime(2026, 4, 1, tzinfo=timezone.utc),
    )


def _seed_config_record(
    *,
    company_code: str,
    competence: str,
    config_version: str,
    event_mappings: list[dict[str, str]] | None = None,
) -> CompanyConfigRecord:
    company_id = f"company:{company_code}"
    payload = CompanyConfig.model_validate(
        {
            "company_code": company_code,
            "company_name": f"Empresa {company_code}",
            "default_process": "11",
            "competence": competence,
            "config_version": config_version,
            "event_mappings": event_mappings or [],
            "employee_mappings": [],
            "pending_policy": {
                "review_required_event_negocios": [],
                "review_required_fields": [],
                "block_on_ambiguous_observations": True,
                "block_on_unmapped_employee": True,
                "block_on_unmapped_event": True,
            },
            "validation_flags": {},
        }
    ).model_dump(mode="json")
    return CompanyConfigRecord(
        id=f"config:{company_id}:{config_version}",
        company_id=company_id,
        version=config_version,
        competence_start=competence,
        competence_end=competence,
        status="active",
        config_payload_internal=payload,
        validated_at=datetime(2026, 4, 1, tzinfo=timezone.utc),
        created_at=datetime(2026, 4, 1, tzinfo=timezone.utc),
        updated_at=datetime(2026, 4, 1, tzinfo=timezone.utc),
    )


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


def test_seed_company_configs_from_missing_issues_creates_batched_configs_and_links_active_config(tmp_path: Path) -> None:
    master_root = tmp_path / "master"
    store = CompanyMasterDataStore(master_root)
    registry_entries = [
        _seed_registry_entry(company_code="3", competence="03/2026", cnpj="00000000000101"),
        _seed_registry_entry(company_code="24", competence="03/2026", cnpj="00000000000202"),
        _seed_registry_entry(company_code="99", competence="04/2026", cnpj="00000000000303"),
    ]
    issues = [
        _seed_missing_issue(company_code="3", competence="03/2026"),
        _seed_missing_issue(company_code="24", competence="03/2026"),
        _seed_missing_issue(company_code="99", competence="04/2026"),
    ]
    store.save_all(registry_entries=registry_entries, config_records=[], issues=issues)

    result = seed_company_configs_from_missing_issues(store_root=master_root)

    refreshed_store = CompanyMasterDataStore(master_root)
    refreshed_companies = {entry.company_code: entry for entry in refreshed_store.load_registry_entries()}
    refreshed_configs = refreshed_store.load_config_records()
    refreshed_issues = refreshed_store.load_issues()

    assert result.default_process == "11"
    assert result.companies_targeted == 3
    assert result.companies_seeded == 3
    assert result.configs_created == 3
    assert result.configs_updated == 0
    assert result.active_config_links_updated == 3
    assert result.issues_resolved == 3
    assert result.remaining_open_company_config_missing == 0
    assert result.exceptions == []
    assert {group.competence for group in result.groups} == {"03/2026", "04/2026"}

    group_032026 = next(group for group in result.groups if group.competence == "03/2026")
    group_042026 = next(group for group in result.groups if group.competence == "04/2026")
    assert group_032026.companies_seeded == 2
    assert group_032026.configs_created == 2
    assert group_032026.active_config_links_updated == 2
    assert group_042026.companies_seeded == 1
    assert group_042026.configs_created == 1
    assert group_042026.active_config_links_updated == 1
    assert group_032026.example_companies

    assert refreshed_companies["3"].active_config_id == "config:company:3:seed-v1-03-2026"
    assert refreshed_companies["24"].active_config_id == "config:company:24:seed-v1-03-2026"
    assert refreshed_companies["99"].active_config_id == "config:company:99:seed-v1-04-2026"
    assert len(refreshed_configs) == 3
    assert all(issue.status == "resolved" for issue in refreshed_issues if issue.issue_type == "company_config_missing")

    resolver = ConfigResolver(registry_root=master_root, legacy_root=tmp_path / "legacy")
    resolved = resolver.resolve(company_code="3", competence="03/2026")
    assert resolved.status == ConfigResolutionStatus.FOUND
    assert resolved.config_source == "registry_company_competence"


def test_seed_event_mappings_from_catalog_adds_standard_mappings_and_keeps_control_company_72_unchanged(tmp_path: Path) -> None:
    master_root = tmp_path / "master"
    store = CompanyMasterDataStore(master_root)
    registry_entries = [
        _seed_registry_entry(
            company_code="3",
            competence="03/2026",
            cnpj="00000000000101",
            active_config_id="config:company:3:seed-v1-03-2026",
        ),
        _seed_registry_entry(
            company_code="24",
            competence="03/2026",
            cnpj="00000000000202",
            active_config_id="config:company:24:seed-v1-03-2026",
        ),
        _seed_registry_entry(
            company_code="99",
            competence="04/2026",
            cnpj="00000000000303",
            active_config_id="config:company:99:seed-v1-04-2026",
        ),
        _seed_registry_entry(
            company_code="72",
            competence="03/2024",
            cnpj="00000000000404",
            active_config_id="config:company:72:cfg-v1",
        ),
    ]
    config_records = [
        _seed_config_record(company_code="3", competence="03/2026", config_version="seed-v1-03-2026"),
        _seed_config_record(company_code="24", competence="03/2026", config_version="seed-v1-03-2026"),
        _seed_config_record(company_code="99", competence="04/2026", config_version="seed-v1-04-2026"),
        CompanyConfigRecord(
            id="config:company:72:cfg-v1",
            company_id="company:72",
            version="cfg-v1",
            competence_start="03/2024",
            competence_end="03/2024",
            status="active",
            config_payload_internal=CompanyConfig.model_validate(
                {
                    "company_code": "72",
                    "company_name": "Dela More",
                    "default_process": "11",
                    "competence": "03/2024",
                    "config_version": "cfg-v1",
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
            ).model_dump(mode="json"),
            validated_at=datetime(2024, 3, 1, tzinfo=timezone.utc),
            created_at=datetime(2024, 3, 1, tzinfo=timezone.utc),
            updated_at=datetime(2024, 3, 1, tzinfo=timezone.utc),
        ),
    ]
    store.save_all(registry_entries=registry_entries, config_records=config_records, issues=[])

    result = seed_event_mappings_from_catalog(store_root=master_root)

    refreshed_store = CompanyMasterDataStore(master_root)
    refreshed_companies = {entry.company_code: entry for entry in refreshed_store.load_registry_entries()}
    refreshed_configs = {record.id: record for record in refreshed_store.load_config_records()}

    standard_mappings = [
        ("gratificacao", "201"),
        ("horas_extras_50", "350"),
    ]

    assert result.configs_targeted == 3
    assert result.configs_updated == 3
    assert result.event_mappings_written == 6
    assert result.active_config_links_updated == 0
    assert result.issues_created == 0
    assert result.exceptions == []
    assert {group.competence for group in result.groups} == {"03/2026", "04/2026"}

    group_032026 = next(group for group in result.groups if group.competence == "03/2026")
    group_042026 = next(group for group in result.groups if group.competence == "04/2026")
    assert group_032026.configs_updated == 2
    assert group_032026.event_mappings_written == 4
    assert group_042026.configs_updated == 1
    assert group_042026.event_mappings_written == 2

    seeded_record_ids = {
        "3": "config:company:3:seed-v1-03-2026",
        "24": "config:company:24:seed-v1-03-2026",
        "99": "config:company:99:seed-v1-04-2026",
    }
    for company_code, record_id in seeded_record_ids.items():
        record = refreshed_configs[record_id]
        config = CompanyConfig.model_validate(record.config_payload_internal)
        assert [(item.event_negocio, item.rubrica_saida) for item in config.event_mappings] == standard_mappings
        assert refreshed_companies[company_code].active_config_id == record.id

    control_record = refreshed_configs["config:company:72:cfg-v1"]
    control_config = CompanyConfig.model_validate(control_record.config_payload_internal)
    assert [(item.event_negocio, item.rubrica_saida) for item in control_config.event_mappings] == [
        ("gratificacao", "201"),
        ("horas_extras_50", "350"),
    ]
    assert refreshed_companies["72"].active_config_id == "config:company:72:cfg-v1"

    resolver = ConfigResolver(registry_root=master_root, legacy_root=tmp_path / "legacy")
    resolved = resolver.resolve(company_code="3", competence="03/2026")
    assert resolved.status == ConfigResolutionStatus.FOUND
    assert resolved.config_source == "registry_company_competence"


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
