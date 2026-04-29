from __future__ import annotations

import json
from pathlib import Path

from openpyxl import Workbook

from dashboard import (
    ConfigResolutionStatus,
    ConfigResolver,
    create_dashboard_run_from_paths,
    run_dashboard_analysis,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
PROFILE_ROOT = REPO_ROOT / "data" / "company_mapping_profiles"


def test_berbella_real_profile_generates_functional_txt_end_to_end(tmp_path: Path) -> None:
    workbook_path = _write_berbella_profile_input(tmp_path / "berbella_profile_input.xlsx")
    registry_root = _write_minimal_registry(tmp_path / "master")
    configs_root = tmp_path / "configs"
    _write_berbella_minimal_config(configs_root)
    paths = create_dashboard_run_from_paths(workbook_path, runs_root=tmp_path / "runs")

    result = run_dashboard_analysis(
        paths,
        config_resolver=ConfigResolver(registry_root=registry_root, legacy_root=configs_root),
        column_profile_root=PROFILE_ROOT,
    )

    assert result.summary.company_code == "887"
    assert result.summary.competence == "03/2026"
    assert result.profile_resolution.status == "found"
    assert result.config_resolution.status == ConfigResolutionStatus.FOUND.value
    assert result.config_resolution.config_source == "legacy_company_competence"
    assert result.summary.serialized_line_count == 1
    assert result.validation_payload["execution"]["status"] == "success"
    assert result.summary.txt_enabled is True
    assert paths.txt_path.exists()
    assert paths.txt_path.read_text(encoding="utf-8").strip()

    normalization_payload = json.loads(paths.normalization_path.read_text(encoding="utf-8"))
    assert normalization_payload["manifest"]["profile"]["company_code"] == "887"
    assert normalization_payload["manifest"]["normalizer"] == "profile_column_mapping"


def _write_berbella_profile_input(path: Path) -> Path:
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "mar 26"
    sheet["A1"] = "BERBELLA LTDA"
    sheet["A2"] = "LANCAMENTOS REFERENTE A FOLHA DO MES - MARCO - 2026"
    headers = [
        "COD.",
        "NOME",
        "H. EXTRA 50% COD.150",
        "H. EXTRA 100% COD.200  FERIADO",
        "AD. NOTURNO COD.25",
        "GORJETA COD.237",
        "ADIANTAMENTO COD. 981",
        "FALTAS             COD. 8792",
        "DSR DE FALTA COD. 8794",
        "ATRASOS       COD. 8069",
        "CONSUMO              COD. 266",
        "VALE TRANSPORTE COD. 48",
        "PLANO ODONT.                               COD. 222",
        "OBSERVACOES dias das faltas",
        "GRAT.",
        "MATRICULA",
    ]
    for column_index, header in enumerate(headers, start=1):
        sheet.cell(row=4, column=column_index, value=header)

    sheet["A6"] = "col-001"
    sheet["B6"] = "Ana Lima"
    sheet["O6"] = "123,45"
    sheet["P6"] = "123"

    path.parent.mkdir(parents=True, exist_ok=True)
    workbook.save(path)
    return path


def _write_minimal_registry(root: Path) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    (root / "companies_registry.json").write_text(
        json.dumps(
            [
                {
                    "active_config_id": None,
                    "cnpj": "44871615000169",
                    "company_code": "887",
                    "created_at": "2026-04-22T19:43:09.804353Z",
                    "default_template_id": "planilha_padrao_folha_v1",
                    "id": "company:887",
                    "is_active": True,
                    "last_competence_seen": "03/2026",
                    "nome_fantasia": "BERBELLA LTDA",
                    "razao_social": "BERBELLA LTDA",
                    "source_import": "resumo_mensal",
                    "status": "active",
                    "updated_at": "2026-04-22T20:08:21.210806Z",
                }
            ],
            ensure_ascii=True,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    (root / "company_configs.json").write_text("[]\n", encoding="utf-8")
    (root / "company_config_issues.json").write_text("[]\n", encoding="utf-8")
    return root


def _write_berbella_minimal_config(root: Path) -> Path:
    payload = {
        "company_code": "887",
        "company_name": "BERBELLA LTDA",
        "competence": "03/2026",
        "config_version": "rodada7-proof",
        "default_process": "11",
        "employee_mappings": [
            {
                "active": True,
                "aliases": [],
                "domain_registration": "123",
                "notes": "Massa minima de prova ponta a ponta da Rodada 7.",
                "source_employee_key": "col-001",
                "source_employee_name": "Ana Lima",
            }
        ],
        "event_mappings": [
            {
                "active": True,
                "event_negocio": "gratificacao",
                "notes": "GRAT. -> 20 conforme perfil real BERBELLA.",
                "rubrica_saida": "20",
            }
        ],
        "notes": "Config minima temporaria para prova end-to-end da Rodada 7.",
        "pending_policy": {
            "block_on_ambiguous_observations": True,
            "block_on_unmapped_employee": True,
            "block_on_unmapped_event": True,
            "notes": None,
            "review_required_event_negocios": [],
            "review_required_fields": [],
        },
        "validation_flags": {},
    }
    path = root / "887" / "03-2026.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path
