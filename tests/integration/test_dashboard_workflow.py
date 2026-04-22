from __future__ import annotations

import json
from pathlib import Path

from openpyxl import load_workbook

from dashboard import (
    apply_workbook_cell_correction,
    create_dashboard_run_from_paths,
    ignore_pending_for_import,
    load_dashboard_run,
    run_dashboard_analysis,
)
from ingestion import save_planilha_padrao_folha_v1


REPO_ROOT = Path(__file__).resolve().parents[2]


def _prepare_config(path: Path) -> Path:
    payload = {
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
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _prepare_single_row_workbook(tmp_path: Path) -> Path:
    workbook_path = tmp_path / "input.xlsx"
    save_planilha_padrao_folha_v1(workbook_path)

    workbook = load_workbook(workbook_path)
    parametros = workbook["PARAMETROS"]
    parametros["B2"] = "72"
    parametros["B3"] = "Dela More"
    parametros["B4"] = "03/2024"
    parametros["B5"] = "mensal"
    parametros["B6"] = "11"
    parametros["B7"] = "v1"

    lancamentos = workbook["LANCAMENTOS_FACEIS"]
    lancamentos["B2"] = "col-999"
    lancamentos["C2"] = "Nova Pessoa"
    lancamentos["H2"] = 100
    workbook.save(workbook_path)
    return workbook_path


def _prepare_non_automatable_workbook(tmp_path: Path) -> tuple[Path, Path]:
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
    lancamentos["N2"] = "revisar"
    workbook.save(workbook_path)

    payload = {
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
    config_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return workbook_path, config_path


def test_dashboard_happy_path_enables_txt(tmp_path: Path) -> None:
    workbook_path = REPO_ROOT / "data" / "golden" / "v1" / "happy_path" / "input.xlsx"
    config_path = REPO_ROOT / "data" / "golden" / "v1" / "happy_path" / "company_config.json"
    paths = create_dashboard_run_from_paths(workbook_path, config_path, runs_root=tmp_path / "runs")

    result = run_dashboard_analysis(paths)
    persisted = load_dashboard_run(paths)

    assert result.summary.txt_enabled is True
    assert result.summary.serialized_line_count == 2
    assert result.summary.pending_count == 0
    assert persisted.summary.txt_enabled is True
    assert persisted.summary.serialized_line_count == 2


def test_dashboard_can_fix_missing_registration_and_reprocess(tmp_path: Path) -> None:
    workbook_path = _prepare_single_row_workbook(tmp_path)
    config_path = _prepare_config(tmp_path / "company_config.json")
    paths = create_dashboard_run_from_paths(workbook_path, config_path, runs_root=tmp_path / "runs")

    initial = run_dashboard_analysis(paths)
    assert initial.summary.txt_enabled is False
    pending = next(item for item in initial.pendings if item.code == "matricula_dominio_ausente")

    apply_workbook_cell_correction(
        paths,
        sheet_name=pending.source_sheet or "",
        cell=pending.source_cell or "",
        new_value="999",
        pending_uid=pending.uid,
    )
    updated = run_dashboard_analysis(paths)

    assert updated.summary.txt_enabled is True
    assert updated.summary.serialized_line_count == 1
    assert not any(item.code == "matricula_dominio_ausente" for item in updated.pendings)


def test_dashboard_can_ignore_non_automatizable_event_and_reprocess(tmp_path: Path) -> None:
    workbook_path, config_path = _prepare_non_automatable_workbook(tmp_path)
    paths = create_dashboard_run_from_paths(workbook_path, config_path, runs_root=tmp_path / "runs")

    initial = run_dashboard_analysis(paths)
    assert initial.summary.pending_count == 1
    pending = next(item for item in initial.pendings if item.code == "evento_nao_automatizavel")

    ignore_pending_for_import(paths, pending)
    updated = run_dashboard_analysis(paths)

    assert updated.summary.pending_count == 0
    assert updated.summary.ignored_count == 1
    assert updated.summary.txt_enabled is True
    workbook = load_workbook(paths.editable_workbook_path)
    assert workbook["LANCAMENTOS_FACEIS"]["N2"].value is None
