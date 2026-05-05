from __future__ import annotations

import json
from pathlib import Path

from dashboard import (
    CompanyEmployeeRecord,
    CompanyEmployeeRegistry,
    CompanyRubricCatalog,
    CompanyRubricRecord,
    RUBRIC_REVIEW_INCOMPLETE,
    analyze_report_import,
    apply_report_employee_suggestions,
    apply_report_rubric_suggestions,
    company_employee_registry_path,
    company_rubric_catalog_path,
    load_company_employee_registry,
    load_company_rubric_catalog,
    parse_report_file,
    save_company_employee_registry,
    save_company_rubric_catalog,
)


def _report_text(*, company_code: str = "900", rubric_line: str | None = None) -> bytes:
    rubric = rubric_line or (
        "Rubrica: 350 - HORAS EXTRAS 50 "
        "Evento canonico: horas_extras_50 Tipo: horas Natureza: provento Total: 12,50"
    )
    return "\n".join(
        [
            f"Empresa: {company_code} - Empresa Relatorio",
            "Competencia: 04/2026",
            "Funcionario: Matricula: 123 Nome: Ana Lima",
            rubric,
        ]
    ).encode("utf-8")


def _dominio_monthly_text(*, company_code: str = "755") -> bytes:
    return "\n".join(
        [
            "EXTRATO MENSAL",
            "Empresa:",
            f"{company_code} - GUSTAVO LOPES LACERDA",
            "CNPJ:",
            "00.000.000/0001-00",
            "Competencia:",
            "03/2026",
            "Contr: 1 GUSTAVO LOPES LACERDA",
            "Empr.: 304 ADILSON RAFAEL DE SOUSA Situação: Trabalhando",
            "200 HORAS EXTRAS 100% 9,04 174,23 P",
            "201 HORAS EXTRAS 50% 17,18 248,33 P",
            "25 ADICIONAL NOTURNO (INFOR) 68,14 154,86 P",
            "8907 QUEBRA DE CAIXA 100,00 100,00 P",
            "204 ADIANTAMENTO ESPORADICO 343,07 343,07 D",
            "8792 DIAS FALTAS 5,00 353,33 D",
            "8794 DIAS FALTAS DSR 2,00 141,33 D",
            "Empr.: 399 ADRIANE MARTINS DE ALMEIDA Situação: Trabalhando",
            "211 PREMIO 108,00 108,00 P 981 DESC.ADIANT.SALARIAL 500,00 500,00 D",
            "Empr.: 412 ALBERT GUILHERME DOS SANTOS COUTINHOSituação: Trabalhando",
        ]
    ).encode("utf-8")


def test_report_importer_parses_dominio_monthly_metadata_and_employees() -> None:
    parsed = parse_report_file(
        file_name="extrato-mensal.txt",
        file_bytes=_dominio_monthly_text(),
    )

    employees = {employee.domain_registration: employee.employee_name for employee in parsed.employees}
    assert parsed.detected_company_code == "755"
    assert parsed.detected_company_name == "GUSTAVO LOPES LACERDA"
    assert parsed.competence == "03/2026"
    assert employees["304"] == "ADILSON RAFAEL DE SOUSA"
    assert employees["399"] == "ADRIANE MARTINS DE ALMEIDA"
    assert employees["412"] == "ALBERT GUILHERME DOS SANTOS COUTINHO"


def test_report_importer_parses_dominio_monthly_rubrics_with_nature() -> None:
    parsed = parse_report_file(
        file_name="extrato-mensal.txt",
        file_bytes=_dominio_monthly_text(),
    )

    rubrics = {rubric.rubric_code: rubric for rubric in parsed.rubrics}
    assert rubrics["200"].description == "HORAS EXTRAS 100%"
    assert rubrics["200"].nature == "provento"
    assert rubrics["200"].value_kind == "horas"
    assert rubrics["201"].description == "HORAS EXTRAS 50%"
    assert rubrics["25"].description == "ADICIONAL NOTURNO (INFOR)"
    assert rubrics["25"].nature == "provento"
    assert rubrics["8907"].description == "QUEBRA DE CAIXA"
    assert rubrics["8907"].nature == "provento"
    assert rubrics["204"].description == "ADIANTAMENTO ESPORADICO"
    assert rubrics["204"].nature == "desconto"
    assert rubrics["8792"].description == "DIAS FALTAS"
    assert rubrics["8792"].nature == "desconto"
    assert rubrics["8792"].value_kind == "quantidade"
    assert rubrics["8794"].description == "DIAS FALTAS DSR"
    assert rubrics["8794"].nature == "desconto"


def test_report_importer_parses_two_dominio_monthly_events_on_same_line() -> None:
    parsed = parse_report_file(
        file_name="extrato-mensal.txt",
        file_bytes="\n".join(
            [
                "EXTRATO MENSAL",
                "755 - GUSTAVO LOPES LACERDA",
                "03/2026",
                "211 PREMIO 108,00 108,00 P 981 DESC.ADIANT.SALARIAL 500,00 500,00 D",
            ]
        ).encode("utf-8"),
    )

    rubrics = {rubric.rubric_code: rubric for rubric in parsed.rubrics}
    assert rubrics["211"].description == "PREMIO"
    assert rubrics["211"].nature == "provento"
    assert rubrics["981"].description == "DESC.ADIANT.SALARIAL"
    assert rubrics["981"].nature == "desconto"


def test_report_importer_does_not_treat_dominio_contractor_as_employee() -> None:
    parsed = parse_report_file(
        file_name="extrato-mensal.txt",
        file_bytes=_dominio_monthly_text(),
    )

    registrations = {employee.domain_registration for employee in parsed.employees}
    assert "1" not in registrations
    assert {"304", "399", "412"} <= registrations


def test_report_importer_dominio_monthly_company_selection_blocking(tmp_path: Path) -> None:
    accepted = analyze_report_import(
        file_name="extrato-mensal.txt",
        file_bytes=_dominio_monthly_text(),
        selected_company_code="755",
        selected_company_name="GUSTAVO LOPES LACERDA",
        employee_registry_root=tmp_path / "employees",
        rubric_catalog_root=tmp_path / "rubrics",
    )
    blocked = analyze_report_import(
        file_name="extrato-mensal.txt",
        file_bytes=_dominio_monthly_text(),
        selected_company_code="900",
        selected_company_name="Outra Empresa",
        employee_registry_root=tmp_path / "employees",
        rubric_catalog_root=tmp_path / "rubrics",
    )

    assert accepted.blocked_reason is None
    assert blocked.is_blocked is True
    assert "diverge" in (blocked.blocked_reason or "")


def test_report_importer_parses_txt_and_does_not_persist_without_apply(tmp_path: Path) -> None:
    employee_root = tmp_path / "employees"
    rubric_root = tmp_path / "rubrics"

    analysis = analyze_report_import(
        file_name="resumo.txt",
        file_bytes=_report_text(),
        selected_company_code="900",
        selected_company_name="Empresa Relatorio",
        employee_registry_root=employee_root,
        rubric_catalog_root=rubric_root,
    )

    assert analysis.blocked_reason is None
    assert analysis.report.detected_company_code == "900"
    assert analysis.report.competence == "04/2026"
    assert analysis.report.employees[0].domain_registration == "123"
    assert analysis.report.rubrics[0].rubric_code == "350"
    assert analysis.report.rubric_totals[0].total_value == "12,50"
    assert not company_employee_registry_path("900", root=employee_root).exists()
    assert not company_rubric_catalog_path("900", root=rubric_root).exists()


def test_report_importer_blocks_divergent_company_before_apply(tmp_path: Path) -> None:
    analysis = analyze_report_import(
        file_name="resumo.txt",
        file_bytes=_report_text(company_code="901"),
        selected_company_code="900",
        employee_registry_root=tmp_path / "employees",
        rubric_catalog_root=tmp_path / "rubrics",
    )

    result = apply_report_employee_suggestions(
        analysis,
        selected_domain_registrations=("123",),
        root=tmp_path / "employees",
    )

    assert analysis.is_blocked is True
    assert "diverge" in (analysis.blocked_reason or "")
    assert result.applied == 0
    assert result.errors


def test_report_importer_applies_selected_employee_with_origin_metadata(tmp_path: Path) -> None:
    employee_root = tmp_path / "employees"
    analysis = analyze_report_import(
        file_name="resumo.txt",
        file_bytes=_report_text(),
        selected_company_code="900",
        selected_company_name="Empresa Relatorio",
        employee_registry_root=employee_root,
        rubric_catalog_root=tmp_path / "rubrics",
    )

    result = apply_report_employee_suggestions(
        analysis,
        selected_domain_registrations=("123",),
        root=employee_root,
    )

    registry = load_company_employee_registry("900", root=employee_root)
    notes = json.loads(registry.employees[0].notes or "{}")
    assert result.applied == 1
    assert registry.employees[0].domain_registration == "123"
    assert registry.employees[0].employee_name == "Ana Lima"
    assert registry.employees[0].source == "dashboard_report_importer"
    assert notes["origem_sugestao"]["arquivo"] == "resumo.txt"
    assert notes["origem_sugestao"]["tipo_dado"] == "funcionario"


def test_report_importer_applies_selected_rubric_with_report_evidence(tmp_path: Path) -> None:
    rubric_root = tmp_path / "rubrics"
    analysis = analyze_report_import(
        file_name="resumo.txt",
        file_bytes=_report_text(),
        selected_company_code="900",
        selected_company_name="Empresa Relatorio",
        employee_registry_root=tmp_path / "employees",
        rubric_catalog_root=rubric_root,
    )

    result = apply_report_rubric_suggestions(
        analysis,
        selected_rubric_codes=("350",),
        root=rubric_root,
    )

    catalog = load_company_rubric_catalog("900", root=rubric_root)
    notes = json.loads(catalog.rubrics[0].notes or "{}")
    assert result.applied == 1
    assert catalog.rubrics[0].rubric_code == "350"
    assert catalog.rubrics[0].canonical_event == "horas_extras_50"
    assert catalog.rubrics[0].value_kind.value == "horas"
    assert catalog.rubrics[0].source == "dashboard_report_importer"
    assert notes["origem_sugestao"]["tipo_dado"] == "rubrica"


def test_report_importer_requires_review_fields_for_new_rubric(tmp_path: Path) -> None:
    rubric_root = tmp_path / "rubrics"
    analysis = analyze_report_import(
        file_name="resumo.txt",
        file_bytes=_report_text(rubric_line="Rubrica: 350 - HORAS EXTRAS 50 Total: 12,50"),
        selected_company_code="900",
        selected_company_name="Empresa Relatorio",
        employee_registry_root=tmp_path / "employees",
        rubric_catalog_root=rubric_root,
    )

    blocked = apply_report_rubric_suggestions(
        analysis,
        selected_rubric_codes=("350",),
        root=rubric_root,
    )
    applied = apply_report_rubric_suggestions(
        analysis,
        selected_rubric_codes=("350",),
        review_overrides={
            "350": {
                "canonical_event": "horas_extras_50",
                "value_kind": "horas",
                "nature": "provento",
            }
        },
        root=rubric_root,
    )

    assert analysis.rubric_reviews[0].status == RUBRIC_REVIEW_INCOMPLETE
    assert blocked.applied == 0
    assert blocked.errors
    assert applied.applied == 1


def test_report_importer_flags_existing_conflicts_without_auto_update(tmp_path: Path) -> None:
    employee_root = tmp_path / "employees"
    rubric_root = tmp_path / "rubrics"
    save_company_employee_registry(
        CompanyEmployeeRegistry(
            company_code="900",
            company_name="Empresa Relatorio",
            employees=[
                CompanyEmployeeRecord(
                    domain_registration="123",
                    employee_name="Ana Antiga",
                    source="test",
                )
            ],
        ),
        root=employee_root,
    )
    save_company_rubric_catalog(
        CompanyRubricCatalog(
            company_code="900",
            company_name="Empresa Relatorio",
            rubrics=[
                CompanyRubricRecord(
                    rubric_code="350",
                    description="HORA EXTRA ANTIGA",
                    canonical_event="horas_extras_50",
                    value_kind="horas",
                    nature="provento",
                    source="test",
                )
            ],
        ),
        root=rubric_root,
    )

    analysis = analyze_report_import(
        file_name="resumo.txt",
        file_bytes=_report_text(),
        selected_company_code="900",
        selected_company_name="Empresa Relatorio",
        employee_registry_root=employee_root,
        rubric_catalog_root=rubric_root,
    )

    registry = load_company_employee_registry("900", root=employee_root)
    catalog = load_company_rubric_catalog("900", root=rubric_root)
    assert analysis.employee_reviews[0].status == "divergente"
    assert analysis.rubric_reviews[0].status == "divergente"
    assert registry.employees[0].employee_name == "Ana Antiga"
    assert catalog.rubrics[0].description == "HORA EXTRA ANTIGA"


def test_report_importer_parses_csv_rows_and_column_profile_suggestion(tmp_path: Path) -> None:
    csv_content = "\n".join(
        [
            (
                "Codigo Empresa,Nome Empresa,Competencia,Matricula Dominio,Nome Funcionario,"
                "Rubrica,Descricao,Evento Canonico,Tipo Valor,Natureza,Total,H. EXTRA 50% COD.150"
            ),
            (
                "900,Empresa Relatorio,04/2026,123,Ana Lima,350,HORAS EXTRAS 50,"
                "horas_extras_50,horas,provento,12.50,1"
            ),
        ]
    )

    analysis = analyze_report_import(
        file_name="resumo.csv",
        file_bytes=csv_content.encode("utf-8"),
        selected_company_code="900",
        selected_company_name="Empresa Relatorio",
        employee_registry_root=tmp_path / "employees",
        rubric_catalog_root=tmp_path / "rubrics",
    )

    assert analysis.report.detected_company_code == "900"
    assert analysis.report.employees[0].employee_name == "Ana Lima"
    assert analysis.report.rubrics[0].rubric_code == "350"
    assert analysis.report.column_profiles[0].column_name == "H. EXTRA 50% COD.150"
    assert analysis.report.column_profiles[0].rubrica_target == "150"
