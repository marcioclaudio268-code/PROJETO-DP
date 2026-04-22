from __future__ import annotations

import json
from pathlib import Path

from openpyxl import load_workbook

from dashboard import (
    ConfigResolutionStatus,
    ConfigResolver,
    DashboardPendingItem,
    apply_workbook_cell_correction,
    create_dashboard_run_from_paths,
    ignore_pending_for_import,
    is_txt_download_enabled,
    load_dashboard_state,
    upsert_employee_mapping_override,
    upsert_event_mapping_override,
)
from ingestion import save_planilha_padrao_folha_v1


def _prepare_workbook_and_config(tmp_path: Path) -> tuple[Path, Path]:
    workbook_path = tmp_path / "input.xlsx"
    config_path = tmp_path / "company_config.json"
    save_planilha_padrao_folha_v1(workbook_path)

    workbook = load_workbook(workbook_path)
    parametros = workbook["PARAMETROS"]
    parametros["B2"] = "72"
    parametros["B3"] = "Dela More"
    parametros["B4"] = "03/2024"
    parametros["B5"] = "mensal"
    parametros["B6"] = "11"
    parametros["B7"] = "v1"

    funcionarios = workbook["FUNCIONARIOS"]
    funcionarios["A2"] = "col-001"
    funcionarios["B2"] = "Ana Lima"
    funcionarios["E2"] = "123"
    funcionarios["H2"] = "ativo"
    funcionarios["I2"] = "sim"

    lancamentos = workbook["LANCAMENTOS_FACEIS"]
    lancamentos["B2"] = "col-001"
    lancamentos["C2"] = "Ana Lima"
    lancamentos["D2"] = "123"
    lancamentos["H2"] = 100
    workbook.save(workbook_path)

    config_payload = {
        "company_code": "72",
        "company_name": "Dela More",
        "default_process": "11",
        "competence": "03/2024",
        "config_version": "cfg-v1",
        "event_mappings": [
            {
                "event_negocio": "gratificacao",
                "rubrica_saida": "201",
            }
        ],
        "employee_mappings": [
            {
                "source_employee_key": "col-001",
                "source_employee_name": "Ana Lima",
                "domain_registration": "123",
            }
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
    config_path.write_text(
        json.dumps(config_payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return workbook_path, config_path


def _write_internal_config(
    root: Path,
    *,
    company_code: str,
    file_name: str,
    competence: str,
    config_version: str = "cfg-v1",
    payload_override: dict | None = None,
) -> Path:
    payload = {
        "company_code": company_code,
        "company_name": "Dela More",
        "default_process": "11",
        "competence": competence,
        "config_version": config_version,
        "event_mappings": [
            {
                "event_negocio": "gratificacao",
                "rubrica_saida": "201",
            }
        ],
        "employee_mappings": [
            {
                "source_employee_key": "col-001",
                "source_employee_name": "Ana Lima",
                "domain_registration": "123",
            }
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
    if payload_override:
        payload.update(payload_override)
    path = root / company_code / file_name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def test_txt_download_enabled_requires_success_and_serialized_lines() -> None:
    validation_payload = {
        "execution": {"status": "success_with_warnings"},
        "fatal_errors": [],
        "inconsistencies": [],
    }
    serialization_payload = {"counts": {"serialized": 2}}

    assert is_txt_download_enabled(
        validation_payload=validation_payload,
        serialization_payload=serialization_payload,
    )

    assert not is_txt_download_enabled(
        validation_payload=validation_payload,
        serialization_payload={"counts": {"serialized": 0}},
    )
    assert not is_txt_download_enabled(
        validation_payload={
            "execution": {"status": "blocked"},
            "fatal_errors": [],
            "inconsistencies": [],
        },
        serialization_payload=serialization_payload,
    )


def test_apply_workbook_cell_correction_updates_workbook_and_records_action(tmp_path: Path) -> None:
    workbook_path, config_path = _prepare_workbook_and_config(tmp_path)
    paths = create_dashboard_run_from_paths(workbook_path, config_path, runs_root=tmp_path / "runs")

    action = apply_workbook_cell_correction(
        paths,
        sheet_name="LANCAMENTOS_FACEIS",
        cell="H2",
        new_value="150,00",
        pending_uid="ingestao:pend-001",
    )

    workbook = load_workbook(paths.editable_workbook_path)
    assert workbook["LANCAMENTOS_FACEIS"]["H2"].value == "150,00"
    state = load_dashboard_state(paths.state_path)
    assert len(state.actions) == 1
    assert state.actions[0].action_id == action.action_id
    assert state.actions[0].payload["cell"] == "H2"


def test_ignore_pending_clears_source_cell_and_records_action(tmp_path: Path) -> None:
    workbook_path, config_path = _prepare_workbook_and_config(tmp_path)
    paths = create_dashboard_run_from_paths(workbook_path, config_path, runs_root=tmp_path / "runs")

    workbook = load_workbook(paths.editable_workbook_path)
    workbook["LANCAMENTOS_FACEIS"]["N2"] = "revisar"
    workbook.save(paths.editable_workbook_path)

    pending = DashboardPendingItem(
        uid="ingestao:pend-vale",
        stage="ingestao",
        pending_id="pend-vale",
        code="evento_nao_automatizavel",
        severity="media",
        employee_name="Ana Lima",
        employee_key="col-001",
        event_name="vale_transporte",
        field_label="vale_transporte",
        found_value="revisar",
        problem="Vale transporte exige revisao.",
        recommended_action="Avaliar manualmente.",
        source_sheet="LANCAMENTOS_FACEIS",
        source_cell="N2",
        source_row=2,
        source_column_name="vale_transporte",
        can_edit_workbook=True,
        can_edit_employee_mapping=False,
        can_edit_event_mapping=False,
        can_ignore=True,
        ignore_mode="evento",
        ignore_label="Ignorar este evento nesta importacao",
    )

    ignore_pending_for_import(paths, pending)

    workbook = load_workbook(paths.editable_workbook_path)
    assert workbook["LANCAMENTOS_FACEIS"]["N2"].value is None
    state = load_dashboard_state(paths.state_path)
    assert state.actions[-1].action_type.value == "ignorar_nesta_importacao"
    assert state.actions[-1].payload["cleared_cells"] == ["N2"]


def test_upsert_event_mapping_override_updates_config(tmp_path: Path) -> None:
    workbook_path, config_path = _prepare_workbook_and_config(tmp_path)
    paths = create_dashboard_run_from_paths(workbook_path, config_path, runs_root=tmp_path / "runs")

    upsert_event_mapping_override(
        paths,
        event_name="horas_extras_50",
        output_rubric="350",
        pending_uid="mapeamento:pend-001",
    )

    payload = json.loads(paths.editable_config_path.read_text(encoding="utf-8"))
    mapping = next(
        item for item in payload["event_mappings"] if item["event_negocio"] == "horas_extras_50"
    )
    assert mapping["rubrica_saida"] == "350"
    assert mapping["active"] is True


def test_upsert_employee_mapping_override_updates_config(tmp_path: Path) -> None:
    workbook_path, config_path = _prepare_workbook_and_config(tmp_path)
    paths = create_dashboard_run_from_paths(workbook_path, config_path, runs_root=tmp_path / "runs")

    upsert_employee_mapping_override(
        paths,
        employee_key="col-002",
        employee_name="Bruno Souza",
        domain_registration="456",
        pending_uid="mapeamento:pend-002",
    )

    payload = json.loads(paths.editable_config_path.read_text(encoding="utf-8"))
    mapping = next(
        item for item in payload["employee_mappings"] if item["source_employee_key"] == "col-002"
    )
    assert mapping["domain_registration"] == "456"
    assert mapping["source_employee_name"] == "Bruno Souza"
    assert mapping["active"] is True


def test_config_resolver_prefers_specific_company_competence(tmp_path: Path) -> None:
    root = tmp_path / "configs"
    _write_internal_config(root, company_code="72", file_name="03-2024.json", competence="03/2024", config_version="cfg-specific")
    _write_internal_config(root, company_code="72", file_name="active.json", competence="01/2024", config_version="cfg-active")

    result = ConfigResolver(registry_root=tmp_path / "master", legacy_root=root).resolve(
        company_code="72", competence="03/2024"
    )

    assert result.status == ConfigResolutionStatus.FOUND
    assert result.config_source == "legacy_company_competence"
    assert result.config_version == "cfg-specific"


def test_config_resolver_returns_not_found_when_no_internal_config_exists(tmp_path: Path) -> None:
    result = ConfigResolver(registry_root=tmp_path / "master", legacy_root=tmp_path / "configs").resolve(
        company_code="72", competence="03/2024"
    )

    assert result.status == ConfigResolutionStatus.NOT_FOUND


def test_config_resolver_returns_ambiguous_when_multiple_specific_candidates_exist(tmp_path: Path) -> None:
    root = tmp_path / "configs"
    _write_internal_config(root, company_code="72", file_name="03-2024.json", competence="03/2024")
    _write_internal_config(root, company_code="72", file_name="03_2024.json", competence="03/2024")

    result = ConfigResolver(registry_root=tmp_path / "master", legacy_root=root).resolve(
        company_code="72", competence="03/2024"
    )

    assert result.status == ConfigResolutionStatus.AMBIGUOUS


def test_config_resolver_returns_mismatch_for_wrong_company(tmp_path: Path) -> None:
    root = tmp_path / "configs"
    _write_internal_config(
        root,
        company_code="72",
        file_name="03-2024.json",
        competence="03/2024",
        payload_override={"company_code": "99"},
    )

    result = ConfigResolver(registry_root=tmp_path / "master", legacy_root=root).resolve(
        company_code="72", competence="03/2024"
    )

    assert result.status == ConfigResolutionStatus.MISMATCH


def test_config_resolver_falls_back_to_active_json(tmp_path: Path) -> None:
    root = tmp_path / "configs"
    _write_internal_config(root, company_code="72", file_name="active.json", competence="01/2024", config_version="cfg-active")

    result = ConfigResolver(registry_root=tmp_path / "master", legacy_root=root).resolve(
        company_code="72", competence="03/2024"
    )

    assert result.status == ConfigResolutionStatus.FOUND
    assert result.config_source == "legacy_company_active"
    assert result.config_version == "cfg-active"
    assert result.config_payload is not None
    assert result.config_payload["competence"] == "03/2024"
