from __future__ import annotations

from pathlib import Path

from openpyxl import Workbook, load_workbook

from dashboard.column_mapping_profiles import (
    ColumnGenerationMode,
    ColumnMappingRule,
    ColumnValueKind,
    CompanyColumnMappingProfile,
)
from dashboard.profile_normalizer import (
    build_canonical_workbook_from_column_profile,
    normalize_workbook_with_column_profile,
)
from ingestion import (
    InputColumnMetadata,
    InputLayoutDetection,
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
    )
    for column_index, header in enumerate(headers, start=1):
        sheet.cell(row=4, column=column_index, value=header)

    row = ("col-001", "Ana Lima", "150,00", "01:30", 2, "Sim", 0, "ferias")
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
    assert lancamentos["H2"].value == "150"
    assert lancamentos["S3"].value == "01:30"
    assert lancamentos["R4"].value == "2"
    assert lancamentos["R5"].value == "2"
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
        "faltas_dias",
    ]
    assert result.movements[0].amount_for_sheet() == "150"
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
