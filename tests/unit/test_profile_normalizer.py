from __future__ import annotations

from pathlib import Path

import pytest
from openpyxl import Workbook, load_workbook

from dashboard.column_mapping_profiles import (
    ColumnGenerationMode,
    ColumnMappingRule,
    ColumnValueKind,
    CompanyColumnMappingProfile,
)
from dashboard.company_employee_registry import (
    CompanyEmployeeRecord,
    CompanyEmployeeRegistry,
    save_company_employee_registry,
)
from dashboard.profile_normalizer import (
    build_canonical_workbook_from_column_profile,
    inspect_workbook_with_position_profile,
    normalize_workbook_with_column_profile,
)
from ingestion import (
    InputColumnMetadata,
    InputLayoutDetection,
    InputLayoutNormalizationError,
    InputWorkbookInspection,
    MONTHLY_LAYOUT_ID,
    ingest_template_v1_workbook,
)


def _build_profile_source_workbook() -> Workbook:
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "mar 26"
    sheet["A1"] = "Empresa Teste"
    sheet["A2"] = "Lancamentos marco 2026"
    headers = (
        "COD.",
        "NOME",
        "GRAT.",
        "ATRASO",
        "FALTA",
        "ADIANT. QUINZ",
        "GRAT. ZERO",
        "GRAT. TEXTO",
        "MATRICULA",
    )
    for column_index, header in enumerate(headers, start=1):
        sheet.cell(row=4, column=column_index, value=header)

    row = ("col-001", "Ana Lima", "150,00", "01:30", 2, "Sim", 0, "ferias", "123")
    for column_index, value in enumerate(row, start=1):
        sheet.cell(row=6, column=column_index, value=value)
    return workbook


def _build_inspection() -> InputWorkbookInspection:
    detection = InputLayoutDetection(
        layout_id=MONTHLY_LAYOUT_ID,
        active_sheet_name="mar 26",
        selected_sheet_name="mar 26",
        selected_sheet_reason="active_sheet",
        company_code="72",
        company_name="Empresa Teste",
        competence="03/2026",
        source_company_name="Empresa Teste",
        source_title_text="Lancamentos marco 2026",
        source_sheet_names=("mar 26",),
        rules_applied=("test_contract",),
    )
    headers = (
        "COD.",
        "NOME",
        "GRAT.",
        "ATRASO",
        "FALTA",
        "ADIANT. QUINZ",
        "GRAT. ZERO",
        "GRAT. TEXTO",
        "MATRICULA",
    )
    columns = tuple(
        InputColumnMetadata(
            column_index=index,
            column_letter=letter,
            column_name=header,
            normalized_column_name=header.lower(),
            header_row=4,
        )
        for index, letter, header in (
            (1, "A", headers[0]),
            (2, "B", headers[1]),
            (3, "C", headers[2]),
            (4, "D", headers[3]),
            (5, "E", headers[4]),
            (6, "F", headers[5]),
            (7, "G", headers[6]),
            (8, "H", headers[7]),
            (9, "I", headers[8]),
        )
    )
    return InputWorkbookInspection(
        layout_id=MONTHLY_LAYOUT_ID,
        company_code="72",
        company_name="Empresa Teste",
        competence="03/2026",
        selected_sheet_name="mar 26",
        source_sheet_names=("mar 26",),
        columns=columns,
        warnings=(),
        detection=detection,
    )


def _build_profile() -> CompanyColumnMappingProfile:
    return CompanyColumnMappingProfile(
        company_code="72",
        company_name="Empresa Teste",
        default_process="11",
        mappings=[
            ColumnMappingRule(
                column_name="GRAT.",
                enabled=True,
                rubrica_target="20",
                value_kind=ColumnValueKind.MONETARY,
                generation_mode=ColumnGenerationMode.SINGLE_LINE,
                ignore_zero=True,
                ignore_text=True,
            ),
            ColumnMappingRule(
                column_name="ATRASO",
                enabled=True,
                rubrica_target="8069",
                value_kind=ColumnValueKind.HOURS,
                generation_mode=ColumnGenerationMode.SINGLE_LINE,
                ignore_zero=True,
                ignore_text=True,
            ),
            ColumnMappingRule(
                column_name="FALTA",
                enabled=True,
                rubricas_target=["8792", "8794"],
                value_kind=ColumnValueKind.QUANTITY,
                generation_mode=ColumnGenerationMode.MULTI_LINE,
                ignore_zero=True,
                ignore_text=True,
            ),
            ColumnMappingRule(
                column_name="ADIANT. QUINZ",
                enabled=False,
                value_kind=ColumnValueKind.MONETARY,
                generation_mode=ColumnGenerationMode.IGNORE,
                ignore_zero=True,
                ignore_text=True,
            ),
            ColumnMappingRule(
                column_name="GRAT. ZERO",
                enabled=True,
                rubrica_target="20",
                value_kind=ColumnValueKind.MONETARY,
                generation_mode=ColumnGenerationMode.SINGLE_LINE,
                ignore_zero=True,
                ignore_text=True,
            ),
            ColumnMappingRule(
                column_name="GRAT. TEXTO",
                enabled=True,
                rubrica_target="20",
                value_kind=ColumnValueKind.MONETARY,
                generation_mode=ColumnGenerationMode.SINGLE_LINE,
                ignore_zero=True,
                ignore_text=True,
            ),
        ],
    )


def _non_empty_launch_rows(workbook) -> list[int]:
    worksheet = workbook["LANCAMENTOS_FACEIS"]
    rows = []
    for row_number in range(2, 30):
        if worksheet[f"B{row_number}"].value is not None:
            rows.append(row_number)
    return rows


def test_profile_normalizer_translates_supported_column_rules_to_canonical_workbook() -> None:
    normalized, manifest = build_canonical_workbook_from_column_profile(
        _build_profile_source_workbook(),
        inspection=_build_inspection(),
        profile=_build_profile(),
    )

    lancamentos = normalized["LANCAMENTOS_FACEIS"]
    assert _non_empty_launch_rows(normalized) == [2, 3, 4, 5]
    assert lancamentos["B2"].value == "col-001"
    assert lancamentos["C2"].value == "Ana Lima"
    assert lancamentos["D2"].value == "123"
    assert lancamentos["H2"].value == "150"
    assert lancamentos["S3"].value == "01:30"
    assert lancamentos["R4"].value == "2"
    assert lancamentos["Y5"].value == "2"
    assert "rubrica_target=8792" in lancamentos["F4"].value
    assert "rubrica_target=8794" in lancamentos["F5"].value

    assert manifest["normalizer"] == "profile_column_mapping"
    assert manifest["counts"]["source_cells_converted"] == 3
    assert manifest["counts"]["generated_movements"] == 4
    assert manifest["counts"]["ignored_cells"] == 1
    assert manifest["counts"]["skipped_zero_cells"] == 1
    assert manifest["counts"]["skipped_text_cells"] == 1


def test_profile_normalizer_output_remains_ingestable_by_v1_loader() -> None:
    normalized, _manifest = build_canonical_workbook_from_column_profile(
        _build_profile_source_workbook(),
        inspection=_build_inspection(),
        profile=_build_profile(),
    )

    result = ingest_template_v1_workbook(normalized)

    assert [movement.event_name for movement in result.movements] == [
        "gratificacao",
        "atrasos_horas",
        "faltas_dias",
        "faltas_dsr",
    ]
    assert result.movements[0].amount_for_sheet() == "150"
    assert result.movements[0].domain_registration == "123"
    assert result.movements[1].quantity_for_sheet() == "01:30"
    assert result.movements[2].quantity_for_sheet() == "2"
    assert result.movements[3].quantity_for_sheet() == "2"


def test_profile_normalizer_persists_canonical_workbook_and_report(tmp_path: Path) -> None:
    source_path = tmp_path / "source.xlsx"
    output_path = tmp_path / "canonical.xlsx"
    report_path = tmp_path / "normalization.json"
    _build_profile_source_workbook().save(source_path)

    result = normalize_workbook_with_column_profile(
        source_path,
        inspection=_build_inspection(),
        profile=_build_profile(),
        output_path=output_path,
        report_path=report_path,
    )

    assert result.workbook_path == output_path
    assert result.report_path == report_path
    assert result.canonical_rows_written == 4
    assert output_path.exists()
    assert report_path.exists()

    persisted = load_workbook(output_path)
    assert persisted["PARAMETROS"]["B2"].value == "72"
    assert persisted["PARAMETROS"]["B6"].value == "11"
    assert persisted["FUNCIONARIOS"]["E2"].value == "123"


def test_profile_normalizer_routes_critical_rubrics_to_distinct_canonical_columns() -> None:
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "mar 26"
    sheet["A1"] = "Empresa Teste"
    headers = (
        "COD.",
        "NOME",
        "EXTRA 70%",
        "EXTRA 100%",
        "EXTRA NOTURNA",
        "DESPESAS",
        "MATRICULA",
    )
    values = ("col-001", "Ana Lima", "01:00", "02:00", "03:00", "10,00", "123")
    for column_index, header in enumerate(headers, start=1):
        sheet.cell(row=4, column=column_index, value=header)
        sheet.cell(row=6, column=column_index, value=values[column_index - 1])

    detection = InputLayoutDetection(
        layout_id=MONTHLY_LAYOUT_ID,
        active_sheet_name="mar 26",
        selected_sheet_name="mar 26",
        selected_sheet_reason="unit_test",
        company_code="72",
        company_name="Empresa Teste",
        competence="03/2026",
        source_company_name="Empresa Teste",
        source_title_text=None,
        source_sheet_names=("mar 26",),
        rules_applied=("unit_test",),
    )
    inspection = InputWorkbookInspection(
        layout_id=MONTHLY_LAYOUT_ID,
        company_code="72",
        company_name="Empresa Teste",
        competence="03/2026",
        selected_sheet_name="mar 26",
        source_sheet_names=("mar 26",),
        columns=tuple(
            InputColumnMetadata(
                column_index=index,
                column_letter=letter,
                column_name=header,
                normalized_column_name=header.lower(),
                header_row=4,
            )
            for index, letter, header in (
                (1, "A", headers[0]),
                (2, "B", headers[1]),
                (3, "C", headers[2]),
                (4, "D", headers[3]),
                (5, "E", headers[4]),
                (6, "F", headers[5]),
                (7, "G", headers[6]),
            )
        ),
        warnings=(),
        detection=detection,
    )
    profile = CompanyColumnMappingProfile(
        company_code="72",
        company_name="Empresa Teste",
        default_process="11",
        mappings=[
            ColumnMappingRule(
                column_name="EXTRA 70%",
                enabled=True,
                rubrica_target="201",
                value_kind=ColumnValueKind.HOURS,
                generation_mode=ColumnGenerationMode.SINGLE_LINE,
                ignore_zero=True,
                ignore_text=True,
            ),
            ColumnMappingRule(
                column_name="EXTRA 100%",
                enabled=True,
                rubrica_target="200",
                value_kind=ColumnValueKind.HOURS,
                generation_mode=ColumnGenerationMode.SINGLE_LINE,
                ignore_zero=True,
                ignore_text=True,
            ),
            ColumnMappingRule(
                column_name="EXTRA NOTURNA",
                enabled=True,
                rubrica_target="25",
                value_kind=ColumnValueKind.HOURS,
                generation_mode=ColumnGenerationMode.SINGLE_LINE,
                ignore_zero=True,
                ignore_text=True,
            ),
            ColumnMappingRule(
                column_name="DESPESAS",
                enabled=True,
                rubrica_target="204",
                value_kind=ColumnValueKind.MONETARY,
                generation_mode=ColumnGenerationMode.SINGLE_LINE,
                ignore_zero=True,
                ignore_text=True,
            ),
        ],
    )

    normalized, _manifest = build_canonical_workbook_from_column_profile(
        workbook,
        inspection=inspection,
        profile=profile,
    )

    lancamentos = normalized["LANCAMENTOS_FACEIS"]
    assert lancamentos["V2"].value == "01:00"
    assert lancamentos["W3"].value == "02:00"
    assert lancamentos["X4"].value == "03:00"
    assert lancamentos["P5"].value == "10"
    assert lancamentos["H2"].value is None


def _build_position_profile(header: str = "HORA 50%") -> CompanyColumnMappingProfile:
    return CompanyColumnMappingProfile(
        company_code="755",
        company_name="GUSTAVO LOPES LACERDA",
        default_process="11",
        mappings=[
            ColumnMappingRule(
                sheet_name="abril",
                header_row=2,
                data_start_row=3,
                employee_code_column="A",
                employee_name_column="B",
                value_column="T",
                expected_header=header,
                enabled=True,
                rubrica_target="201",
                value_kind="horas",
                nature="provento",
                generation_mode="single_line",
                ignore_zero=True,
                ignore_text=True,
                status="active",
            )
        ],
    )


def _build_position_workbook(
    header: str = "HORA 50%",
    *,
    employee_code_header: str = "CODIGO",
    employee_name_header: str = "NOME",
    employee_code_value: str = "304",
    employee_name_value: str = "ADILSON RAFAEL DE SOUSA",
) -> Workbook:
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "abril"
    sheet["A2"] = employee_code_header
    sheet["B2"] = employee_name_header
    sheet["T2"] = header
    sheet["A3"] = employee_code_value
    sheet["B3"] = employee_name_value
    sheet["T3"] = "01:30"
    return workbook


def _write_active_registry(root: Path, employees: list[CompanyEmployeeRecord]) -> None:
    save_company_employee_registry(
        CompanyEmployeeRegistry(
            company_code="755",
            company_name="GUSTAVO LOPES LACERDA",
            employees=employees,
        ),
        root=root,
    )


def test_position_profile_validates_expected_header_and_generates_rubric_column(tmp_path: Path) -> None:
    workbook_path = tmp_path / "fechamento.xlsx"
    _build_position_workbook().save(workbook_path)
    profile = _build_position_profile()
    inspection = inspect_workbook_with_position_profile(
        workbook_path,
        profile=profile,
        selected_company_code="755",
        selected_company_name="GUSTAVO LOPES LACERDA",
        selected_competence="04/2026",
    )

    normalized, manifest = build_canonical_workbook_from_column_profile(
        load_workbook(workbook_path),
        inspection=inspection,
        profile=profile,
        employee_registry_root=tmp_path / "employees",
    )

    assert inspection.columns[0].column_letter == "T"
    assert manifest["counts"]["generated_movements"] == 1
    assert normalized["FUNCIONARIOS"]["E2"].value == "304"
    assert normalized["LANCAMENTOS_FACEIS"]["V2"].value == "01:30"


def test_position_profile_blocks_when_expected_header_differs(tmp_path: Path) -> None:
    workbook_path = tmp_path / "fechamento.xlsx"
    _build_position_workbook(header="HORAS 100%").save(workbook_path)
    profile = _build_position_profile(header="HORA 50%")
    inspection = inspect_workbook_with_position_profile(
        workbook_path,
        profile=profile,
        selected_company_code="755",
        selected_company_name="GUSTAVO LOPES LACERDA",
        selected_competence="04/2026",
    )

    with pytest.raises(InputLayoutNormalizationError) as exc_info:
        build_canonical_workbook_from_column_profile(
            load_workbook(workbook_path),
            inspection=inspection,
            profile=profile,
            employee_registry_root=tmp_path / "employees",
        )

    assert exc_info.value.code == "cabecalho_perfil_divergente"
    assert "A coluna T esperava HORA 50%" in str(exc_info.value)


def test_position_profile_resolves_employee_by_name_when_registration_is_blank(tmp_path: Path) -> None:
    workbook_path = tmp_path / "fechamento.xlsx"
    _build_position_workbook(employee_code_value="", employee_name_value="ADILSON RAFAEL DE SOUSA").save(workbook_path)
    _write_active_registry(
        tmp_path / "employees",
        [
            CompanyEmployeeRecord(
                employee_name="ADILSON RAFAEL DE SOUSA",
                domain_registration="304",
                source="test",
            )
        ],
    )
    profile = _build_position_profile()
    inspection = inspect_workbook_with_position_profile(
        workbook_path,
        profile=profile,
        selected_company_code="755",
        selected_company_name="GUSTAVO LOPES LACERDA",
        selected_competence="04/2026",
    )

    normalized, _manifest = build_canonical_workbook_from_column_profile(
        load_workbook(workbook_path),
        inspection=inspection,
        profile=profile,
        employee_registry_root=tmp_path / "employees",
    )

    assert normalized["FUNCIONARIOS"]["B2"].value == "ADILSON RAFAEL DE SOUSA"
    assert normalized["FUNCIONARIOS"]["E2"].value == "304"
    assert normalized["LANCAMENTOS_FACEIS"]["D2"].value == "304"


def test_position_profile_resolves_employee_by_name_when_registration_column_has_text(tmp_path: Path) -> None:
    workbook_path = tmp_path / "fechamento.xlsx"
    _build_position_workbook(
        employee_code_value="ADILSON RAFAEL DE SOUSA",
        employee_name_value=" Adilson  Rafael de Sousa ",
    ).save(workbook_path)
    _write_active_registry(
        tmp_path / "employees",
        [
            CompanyEmployeeRecord(
                employee_name="ADILSON RAFAEL DE SOUSA",
                domain_registration="304",
                source="test",
            )
        ],
    )
    profile = _build_position_profile()
    inspection = inspect_workbook_with_position_profile(
        workbook_path,
        profile=profile,
        selected_company_code="755",
        selected_company_name="GUSTAVO LOPES LACERDA",
        selected_competence="04/2026",
    )

    normalized, _manifest = build_canonical_workbook_from_column_profile(
        load_workbook(workbook_path),
        inspection=inspection,
        profile=profile,
        employee_registry_root=tmp_path / "employees",
    )

    assert normalized["FUNCIONARIOS"]["E2"].value == "304"
    assert normalized["FUNCIONARIOS"]["B2"].value == "ADILSON RAFAEL DE SOUSA"


def test_position_profile_blocks_when_employee_name_is_not_found(tmp_path: Path) -> None:
    workbook_path = tmp_path / "fechamento.xlsx"
    _build_position_workbook(employee_code_value="", employee_name_value="FUNCIONARIO INEXISTENTE").save(workbook_path)
    _write_active_registry(
        tmp_path / "employees",
        [
            CompanyEmployeeRecord(
                employee_name="ADILSON RAFAEL DE SOUSA",
                domain_registration="304",
                source="test",
            )
        ],
    )
    profile = _build_position_profile()
    inspection = inspect_workbook_with_position_profile(
        workbook_path,
        profile=profile,
        selected_company_code="755",
        selected_company_name="GUSTAVO LOPES LACERDA",
        selected_competence="04/2026",
    )

    with pytest.raises(InputLayoutNormalizationError) as exc_info:
        build_canonical_workbook_from_column_profile(
            load_workbook(workbook_path),
            inspection=inspection,
            profile=profile,
            employee_registry_root=tmp_path / "employees",
        )

    assert exc_info.value.code == "funcionario_nome_nao_encontrado"
    assert "Funcionario 'FUNCIONARIO INEXISTENTE' nao encontrado no cadastro ativo da empresa." in str(exc_info.value)


def test_position_profile_blocks_when_employee_name_is_ambiguous(tmp_path: Path) -> None:
    workbook_path = tmp_path / "fechamento.xlsx"
    _build_position_workbook(employee_code_value="", employee_name_value="ADILSON RAFAEL DE SOUSA").save(workbook_path)
    _write_active_registry(
        tmp_path / "employees",
        [
            CompanyEmployeeRecord(
                employee_name="ADILSON RAFAEL DE SOUSA",
                domain_registration="304",
                source="test",
            ),
            CompanyEmployeeRecord(
                employee_name="ADILSON RAFAEL DE SOUSA",
                domain_registration="999",
                aliases=["ADILSON RAFAEL DE SOUSA"],
                source="test",
            ),
        ],
    )
    profile = _build_position_profile()
    inspection = inspect_workbook_with_position_profile(
        workbook_path,
        profile=profile,
        selected_company_code="755",
        selected_company_name="GUSTAVO LOPES LACERDA",
        selected_competence="04/2026",
    )

    with pytest.raises(InputLayoutNormalizationError) as exc_info:
        build_canonical_workbook_from_column_profile(
            load_workbook(workbook_path),
            inspection=inspection,
            profile=profile,
            employee_registry_root=tmp_path / "employees",
        )

    assert exc_info.value.code == "funcionario_nome_ambiguo"
    assert "encontrou mais de um cadastro compativel" in str(exc_info.value)


def test_position_profile_blocks_without_valid_registration_and_without_name_column(tmp_path: Path) -> None:
    workbook_path = tmp_path / "fechamento.xlsx"
    _build_position_workbook(employee_code_value="ADILSON RAFAEL DE SOUSA").save(workbook_path)
    profile = CompanyColumnMappingProfile(
        company_code="755",
        company_name="GUSTAVO LOPES LACERDA",
        default_process="11",
        mappings=[
            ColumnMappingRule(
                sheet_name="abril",
                header_row=2,
                data_start_row=3,
                employee_code_column="A",
                value_column="T",
                expected_header="HORA 50%",
                enabled=True,
                rubrica_target="201",
                value_kind="horas",
                nature="provento",
                generation_mode="single_line",
                ignore_zero=True,
                ignore_text=True,
                status="active",
            )
        ],
    )
    inspection = inspect_workbook_with_position_profile(
        workbook_path,
        profile=profile,
        selected_company_code="755",
        selected_company_name="GUSTAVO LOPES LACERDA",
        selected_competence="04/2026",
    )

    with pytest.raises(InputLayoutNormalizationError) as exc_info:
        build_canonical_workbook_from_column_profile(
            load_workbook(workbook_path),
            inspection=inspection,
            profile=profile,
            employee_registry_root=tmp_path / "employees",
        )

    assert exc_info.value.code == "perfil_colunas_sem_nome_para_resolver_funcionario"
    assert "Linha sem matricula valida e sem coluna de nome configurada no perfil." in str(exc_info.value)
