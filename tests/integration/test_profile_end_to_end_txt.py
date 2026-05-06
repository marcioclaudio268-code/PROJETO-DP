from __future__ import annotations

import json
from pathlib import Path

import pytest
from openpyxl import Workbook

from dashboard import (
    ConfigResolutionStatus,
    ConfigResolver,
    create_dashboard_run_from_paths,
    run_dashboard_analysis,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
PROFILE_ROOT = REPO_ROOT / "data" / "company_mapping_profiles"

COMPANY_NAMES = {
    "887": "BERBELLA LTDA",
    "1016": "MAIS QUE BOLO DOCES E SALGADOS LTDA",
    "448": "SAAD E TOSSI LTDA ME",
}


CRITICAL_COLUMN_CASES = (
    pytest.param("887", "EXTRA 70%", "01:00", (("horas_extras_70", "201"),), id="berbella-extra-70-201"),
    pytest.param("448", "EXTRA 70%", "01:00", (("horas_extras_70", "219"),), id="saad-extra-70-219"),
    pytest.param("887", "EXTRA 100%", "02:00", (("horas_extras_100", "200"),), id="extra-100-200"),
    pytest.param("887", "EXTRA NOTURNA", "03:00", (("hora_extra_noturna", "25"),), id="berbella-extra-noturna-25"),
    pytest.param("1016", "HORA EXTRA NOTURNA", "03:00", (("hora_extra_noturna", "25"),), id="mqb-hora-extra-noturna-25"),
    pytest.param("448", "HORA EXTRA NOTURNA 100%", "03:00", (("hora_extra_noturna", "25"),), id="saad-hora-extra-noturna-100-25"),
    pytest.param("887", "ATRASO", "00:30", (("atrasos_horas", "8069"),), id="atraso-8069"),
    pytest.param("887", "FALTA", 1, (("faltas_dias", "8792"), ("faltas_dsr", "8794")), id="falta-composta"),
    pytest.param("887", "DESPESAS", "10,00", (("mercadoria", "204"),), id="berbella-despesas-204"),
    pytest.param("1016", "DESPESAS", "10,00", (("mercadoria", "202"),), id="mqb-despesas-202"),
    pytest.param("448", "DESPESAS", "10,00", (("mercadoria", "264"),), id="saad-despesas-264"),
)


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


@pytest.mark.parametrize(
    ("company_code", "column_name", "source_value", "expected_event_rubrics"),
    CRITICAL_COLUMN_CASES,
)
def test_real_profile_critical_columns_generate_expected_final_rubrics(
    tmp_path: Path,
    company_code: str,
    column_name: str,
    source_value: object,
    expected_event_rubrics: tuple[tuple[str, str], ...],
) -> None:
    company_name = COMPANY_NAMES[company_code]
    workbook_path = _write_profile_input(
        tmp_path / f"{company_code}_{column_name.replace(' ', '_')}.xlsx",
        company_name=company_name,
        column_name=column_name,
        source_value=source_value,
    )
    registry_root = _write_minimal_registry(tmp_path / "master", company_code=company_code, company_name=company_name)
    configs_root = tmp_path / "configs"
    _write_minimal_config(
        configs_root,
        company_code=company_code,
        company_name=company_name,
        event_mappings=expected_event_rubrics,
    )
    paths = create_dashboard_run_from_paths(workbook_path, runs_root=tmp_path / "runs")

    result = run_dashboard_analysis(
        paths,
        config_resolver=ConfigResolver(registry_root=registry_root, legacy_root=configs_root),
        column_profile_root=PROFILE_ROOT,
    )

    expected_rubrics = tuple(rubric for _event_name, rubric in expected_event_rubrics)
    mapped_payload = json.loads(paths.mapped_artifact_path.read_text(encoding="utf-8"))

    assert result.profile_resolution.status == "found"
    assert result.config_resolution.status == ConfigResolutionStatus.FOUND.value
    assert result.summary.serialized_line_count == len(expected_rubrics)
    assert result.validation_payload["execution"]["status"] == "success"
    assert result.summary.txt_enabled is True
    assert _event_rubrics_from_mapped_payload(mapped_payload) == expected_event_rubrics
    assert _rubrics_from_txt(paths.txt_path) == expected_rubrics
    assert "gratificacao" not in {
        event_name for event_name, _rubric in expected_event_rubrics if column_name == "EXTRA 70%"
    }


def _write_berbella_profile_input(path: Path) -> Path:
    return _write_profile_input(
        path,
        company_name="BERBELLA LTDA",
        column_name="GRAT.",
        source_value="123,45",
    )


def _write_profile_input(
    path: Path,
    *,
    company_name: str,
    column_name: str,
    source_value: object,
) -> Path:
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "mar 26"
    sheet["A1"] = company_name
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
        column_name,
        "MATRICULA",
    ]
    for column_index, header in enumerate(headers, start=1):
        sheet.cell(row=4, column=column_index, value=header)

    sheet["A6"] = "col-001"
    sheet["B6"] = "Ana Lima"
    sheet["O6"] = source_value
    sheet["P6"] = "123"

    path.parent.mkdir(parents=True, exist_ok=True)
    workbook.save(path)
    return path


def _write_minimal_registry(
    root: Path,
    *,
    company_code: str = "887",
    company_name: str = "BERBELLA LTDA",
) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    (root / "companies_registry.json").write_text(
        json.dumps(
            [
                {
                    "active_config_id": None,
                    "cnpj": None,
                    "company_code": company_code,
                    "created_at": "2026-04-22T19:43:09.804353Z",
                    "default_template_id": "planilha_padrao_folha_v1",
                    "id": f"company:{company_code}",
                    "is_active": True,
                    "last_competence_seen": "03/2026",
                    "nome_fantasia": company_name,
                    "razao_social": company_name,
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
    return _write_minimal_config(
        root,
        company_code="887",
        company_name="BERBELLA LTDA",
        event_mappings=(("gratificacao", "20"),),
        config_version="rodada7-proof",
        notes="Config minima temporaria para prova end-to-end da Rodada 7.",
    )


def _write_minimal_config(
    root: Path,
    *,
    company_code: str,
    company_name: str,
    event_mappings: tuple[tuple[str, str], ...],
    config_version: str = "rodada7b-proof",
    notes: str = "Config minima temporaria para prova de colunas criticas da Rodada 7B.",
) -> Path:
    payload = {
        "company_code": company_code,
        "company_name": company_name,
        "competence": "03/2026",
        "config_version": config_version,
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
                "event_negocio": event_name,
                "notes": "Mapping minimo de teste para prova e2e.",
                "rubrica_saida": rubric,
            }
            for event_name, rubric in event_mappings
        ],
        "notes": notes,
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
    path = root / company_code / "03-2026.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _event_rubrics_from_mapped_payload(payload: dict) -> tuple[tuple[str, str], ...]:
    return tuple(
        (movement["event_name"], movement["output_rubric"])
        for movement in payload["mapped_movements"]
    )


def _rubrics_from_txt(path: Path) -> tuple[str, ...]:
    lines = [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    return tuple(line[18:22].lstrip("0") or "0" for line in lines)
