"""Profile-driven conversion from uploaded workbooks into the V1 canonical workbook."""

from __future__ import annotations

import json
import re
import unicodedata
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

from openpyxl import load_workbook
from openpyxl.workbook import Workbook
from openpyxl.worksheet.worksheet import Worksheet

from domain import NormalizedHours, decimal_to_plain_string
from ingestion import (
    InputColumnMetadata,
    InputLayoutNormalizationError,
    InputNormalizationResult,
    InputWorkbookInspection,
    NormalizationError,
    is_empty_value,
    normalize_hours_hhmm,
    normalize_money_brl,
    normalize_quantity,
    normalized_optional_text,
)
from ingestion.input_layout import (
    DEFAULT_CANONICAL_LAYOUT_VERSION,
    DEFAULT_CANONICAL_PAYROLL_TYPE,
    MONTHLY_LAYOUT_ID,
)
from ingestion.template_v1 import MAX_DATA_ROWS, create_planilha_padrao_folha_v1

from .column_mapping_profiles import (
    ColumnGenerationMode,
    ColumnMappingRule,
    ColumnValueKind,
    CompanyColumnMappingProfile,
)


RUBRIC_TO_CANONICAL_EVENT_COLUMN = {
    "20": "gratificacao",
    "201": "gratificacao",
    "150": "horas_extras_50",
    "350": "horas_extras_50",
    "8069": "atrasos_horas",
    "8792": "faltas_dias",
    "8794": "faltas_dias",
    "266": "mercadoria",
    "981": "desconto_adiantamento",
    "48": "vale_transporte",
}

CANONICAL_EVENT_COLUMNS = {
    "horas_extras_50": "G",
    "gratificacao": "H",
    "vale_transporte": "N",
    "mercadoria": "P",
    "faltas_dias": "R",
    "atrasos_horas": "S",
    "desconto_adiantamento": "T",
}

STATUS_TOKENS = {
    "ferias": "ferias",
    "afastado": "afastado",
    "licenca": "afastado",
    "rescisao": "rescindido",
    "ignorar": "ignorar",
}
IDENTITY_COLUMN_TOKENS = {
    "matricula",
    "matricula dominio",
    "registro",
    "registro dominio",
}


@dataclass(frozen=True, slots=True)
class _ParsedProfileValue:
    value_for_workbook: str
    is_zero: bool


def normalize_workbook_with_column_profile(
    input_path: str | Path,
    *,
    inspection: InputWorkbookInspection,
    profile: CompanyColumnMappingProfile,
    output_path: str | Path | None = None,
    report_path: str | Path | None = None,
    max_data_rows: int = MAX_DATA_ROWS,
) -> InputNormalizationResult:
    """Convert an inspected non-canonical workbook into the V1 canonical workbook."""

    source_path = Path(input_path)
    workbook = load_workbook(source_path)
    normalized_workbook, manifest = build_canonical_workbook_from_column_profile(
        workbook,
        inspection=inspection,
        profile=profile,
        max_data_rows=max_data_rows,
    )

    target_path = Path(output_path) if output_path is not None else source_path
    target_path.parent.mkdir(parents=True, exist_ok=True)
    normalized_workbook.save(target_path)

    report_target = Path(report_path) if report_path is not None else None
    result = InputNormalizationResult(
        source_path=source_path,
        workbook_path=target_path,
        report_path=report_target,
        layout=inspection.detection,
        canonical_rows_written=int(manifest["counts"]["canonical_rows_written"]),
        employee_rows_written=int(manifest["counts"]["employee_rows_written"]),
        supported_cells_written=int(manifest["counts"]["source_cells_converted"]),
        unsupported_cells_preserved=int(
            manifest["counts"]["ignored_cells"] + manifest["counts"]["skipped_text_cells"]
        ),
        orphan_note_rows=0,
        manifest=manifest,
    )
    if report_target is not None:
        report_target.parent.mkdir(parents=True, exist_ok=True)
        report_target.write_text(
            json.dumps(result.as_dict(), ensure_ascii=True, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    return result


def build_canonical_workbook_from_column_profile(
    workbook: Workbook,
    *,
    inspection: InputWorkbookInspection,
    profile: CompanyColumnMappingProfile,
    max_data_rows: int = MAX_DATA_ROWS,
) -> tuple[Workbook, dict]:
    if inspection.layout_id == "template_v1_canonico":
        raise InputLayoutNormalizationError(
            "perfil_colunas_nao_requerido",
            "Layout canonico V1 nao deve ser convertido por perfil de colunas.",
            source=inspection.selected_sheet_name,
        )
    if profile.company_code != inspection.company_code:
        raise InputLayoutNormalizationError(
            "perfil_empresa_divergente",
            "Perfil de colunas pertence a empresa diferente da planilha inspecionada.",
            source=inspection.selected_sheet_name,
            details={"inspection_company_code": inspection.company_code, "profile_company_code": profile.company_code},
        )
    if inspection.selected_sheet_name not in workbook.sheetnames:
        raise InputLayoutNormalizationError(
            "aba_detectada_ausente",
            f"A aba detectada '{inspection.selected_sheet_name}' nao existe no workbook.",
            source=inspection.selected_sheet_name,
        )

    selected_sheet = workbook[inspection.selected_sheet_name]
    normalized_workbook = create_planilha_padrao_folha_v1(max_data_rows=max_data_rows)
    _populate_parameters(normalized_workbook, inspection, profile)

    funcionarios = normalized_workbook["FUNCIONARIOS"]
    lancamentos = normalized_workbook["LANCAMENTOS_FACEIS"]
    column_mappings = _match_profile_mappings_to_columns(profile, inspection.columns)
    identity_column = _find_domain_registration_column(inspection.columns)

    employee_rows_written = 0
    canonical_rows_written = 0
    source_cells_converted = 0
    ignored_cells = 0
    skipped_zero_cells = 0
    skipped_text_cells = 0
    generated_movements = 0

    header_row = min((column.header_row for column in inspection.columns), default=4)
    for source_row in range(header_row + 1, selected_sheet.max_row + 1):
        row_values = {
            column_index: selected_sheet.cell(row=source_row, column=column_index).value
            for column_index in range(1, selected_sheet.max_column + 1)
        }
        if _row_is_empty(row_values):
            continue

        employee_key = normalized_optional_text(row_values.get(1))
        employee_name = normalized_optional_text(row_values.get(2))
        domain_registration = (
            normalized_optional_text(row_values.get(identity_column.column_index))
            if identity_column is not None
            else None
        )
        if employee_key is None and employee_name is None:
            if _row_has_profile_data(row_values, column_mappings):
                raise InputLayoutNormalizationError(
                    "linha_orfa_com_dado_critico",
                    "Linha com coluna mapeada preenchida nao possui identificacao de colaborador.",
                    source=f"{selected_sheet.title}!A{source_row}",
                )
            continue

        employee_status, employee_note = _infer_employee_status(row_values)
        employee_rows_written += 1
        _write_employee_row(
            funcionarios,
            employee_rows_written + 1,
            employee_key=employee_key,
            employee_name=employee_name,
            domain_registration=domain_registration,
            status_colaborador=employee_status,
            note=employee_note,
            source_sheet=selected_sheet.title,
            source_row=source_row,
        )

        for column, mapping in column_mappings:
            raw_value = row_values.get(column.column_index)
            if is_empty_value(raw_value):
                continue
            if mapping.generation_mode == ColumnGenerationMode.IGNORE:
                ignored_cells += 1
                continue

            parsed_value = _parse_profile_value(
                raw_value,
                mapping=mapping,
                source=f"{selected_sheet.title}!{column.column_letter}{source_row}",
            )
            if parsed_value is None:
                skipped_text_cells += 1
                continue
            if parsed_value.is_zero and mapping.ignore_zero:
                skipped_zero_cells += 1
                continue

            source_cells_converted += 1
            for target_rubric in mapping.target_rubrics:
                event_column = _canonical_event_column_for_rubric(
                    target_rubric,
                    source=f"{selected_sheet.title}!{column.column_letter}{source_row}",
                )
                canonical_rows_written += 1
                generated_movements += 1
                _write_launch_row(
                    lancamentos,
                    canonical_rows_written + 1,
                    event_column=event_column,
                    value=parsed_value.value_for_workbook,
                    employee_key=employee_key,
                    employee_name=employee_name,
                    domain_registration=domain_registration,
                    observation=_build_launch_observation(
                        inspection=inspection,
                        source_row=source_row,
                        source_column=column,
                        target_rubric=target_rubric,
                        profile=profile,
                    ),
                )

    manifest = {
        "layout_id": inspection.layout_id,
        "normalizer": "profile_column_mapping",
        "selected_sheet_name": inspection.selected_sheet_name,
        "selected_sheet_reason": inspection.detection.selected_sheet_reason,
        "company_code": inspection.company_code,
        "company_name": inspection.company_name,
        "competence": inspection.competence,
        "source_company_name": inspection.detection.source_company_name,
        "source_title_text": inspection.detection.source_title_text,
        "source_sheet_names": list(inspection.source_sheet_names),
        "rules_applied": [*inspection.detection.rules_applied, "profile_column_mapping"],
        "warnings": list(inspection.warnings),
        "profile": {
            "company_code": profile.company_code,
            "company_name": profile.company_name,
            "profile_version": profile.profile_version,
            "default_process": profile.default_process,
        },
        "columns": [
            {
                "source_column": column.column_name,
                "source_column_letter": column.column_letter,
                "generation_mode": mapping.generation_mode.value,
                "target_rubrics": list(mapping.target_rubrics),
                "value_kind": mapping.value_kind.value,
            }
            for column, mapping in column_mappings
        ],
        "counts": {
            "employee_rows_written": employee_rows_written,
            "canonical_rows_written": canonical_rows_written,
            "source_cells_converted": source_cells_converted,
            "generated_movements": generated_movements,
            "ignored_cells": ignored_cells,
            "skipped_zero_cells": skipped_zero_cells,
            "skipped_text_cells": skipped_text_cells,
        },
        "row_warnings": [],
    }
    return normalized_workbook, manifest


def _match_profile_mappings_to_columns(
    profile: CompanyColumnMappingProfile,
    columns: tuple[InputColumnMetadata, ...],
) -> tuple[tuple[InputColumnMetadata, ColumnMappingRule], ...]:
    mapping_index: dict[str, ColumnMappingRule] = {}
    for mapping in profile.mappings:
        for token in (mapping.column_key, mapping.column_name):
            if token:
                mapping_index[_normalize_token(token)] = mapping

    matches: list[tuple[InputColumnMetadata, ColumnMappingRule]] = []
    for column in columns:
        tokens = (
            column.column_name,
            column.normalized_column_name,
            column.column_letter,
            str(column.column_index),
        )
        mapping = next((mapping_index[_normalize_token(token)] for token in tokens if _normalize_token(token) in mapping_index), None)
        if mapping is not None:
            matches.append((column, mapping))
    return tuple(matches)


def is_profile_identity_column(column: InputColumnMetadata) -> bool:
    return _normalize_token(column.column_name) in IDENTITY_COLUMN_TOKENS


def _find_domain_registration_column(
    columns: tuple[InputColumnMetadata, ...],
) -> InputColumnMetadata | None:
    return next((column for column in columns if is_profile_identity_column(column)), None)


def _parse_profile_value(
    value: object,
    *,
    mapping: ColumnMappingRule,
    source: str,
) -> _ParsedProfileValue | None:
    try:
        if mapping.value_kind == ColumnValueKind.MONETARY:
            amount = normalize_money_brl(value)
            return _ParsedProfileValue(decimal_to_plain_string(amount), amount == Decimal("0"))
        if mapping.value_kind == ColumnValueKind.QUANTITY:
            quantity = normalize_quantity(value)
            return _ParsedProfileValue(decimal_to_plain_string(quantity), quantity == Decimal("0"))
        if mapping.value_kind == ColumnValueKind.HOURS:
            hours = _normalize_profile_hours(value)
            return _ParsedProfileValue(hours.text, hours.total_minutes == 0)
    except NormalizationError as exc:
        if mapping.ignore_text:
            return None
        raise InputLayoutNormalizationError(
            "valor_perfil_invalido",
            f"Valor nao pode ser convertido pela regra da coluna '{mapping.source_column_id}': {exc}",
            source=source,
        ) from exc

    raise InputLayoutNormalizationError(
        "tipo_valor_perfil_nao_suportado",
        f"Tipo de valor nao suportado no perfil: {mapping.value_kind}.",
        source=source,
    )


def _normalize_profile_hours(value: object) -> NormalizedHours:
    if isinstance(value, int):
        return normalize_hours_hhmm(f"{value:02d}:00")
    if isinstance(value, float) and value.is_integer():
        return normalize_hours_hhmm(f"{int(value):02d}:00")
    return normalize_hours_hhmm(value)


def _canonical_event_column_for_rubric(target_rubric: str, *, source: str) -> str:
    normalized_rubric = str(target_rubric).strip()
    event_name = RUBRIC_TO_CANONICAL_EVENT_COLUMN.get(normalized_rubric)
    if event_name is None:
        raise InputLayoutNormalizationError(
            "rubrica_sem_coluna_canonica",
            f"Rubrica '{target_rubric}' nao possui coluna canonica V1 suportada nesta rodada.",
            source=source,
        )
    return CANONICAL_EVENT_COLUMNS[event_name]


def _populate_parameters(
    workbook: Workbook,
    inspection: InputWorkbookInspection,
    profile: CompanyColumnMappingProfile,
) -> None:
    worksheet = workbook["PARAMETROS"]
    _set_parameter_value(worksheet, "empresa_codigo", inspection.company_code)
    _set_parameter_value(worksheet, "empresa_nome", inspection.company_name)
    _set_parameter_value(worksheet, "competencia", inspection.competence)
    _set_parameter_value(worksheet, "tipo_folha", DEFAULT_CANONICAL_PAYROLL_TYPE)
    _set_parameter_value(worksheet, "processo_padrao", profile.default_process or "11")
    _set_parameter_value(worksheet, "versao_layout", DEFAULT_CANONICAL_LAYOUT_VERSION)
    _set_parameter_value(worksheet, "responsavel_preenchimento", "profile_column_mapping")
    _set_parameter_value(worksheet, "observacoes_gerais", f"normalizado_por_perfil; layout={inspection.layout_id}")


def _set_parameter_value(worksheet: Worksheet, field_name: str, value: object) -> None:
    for row_number in range(2, worksheet.max_row + 1):
        current = normalized_optional_text(worksheet.cell(row=row_number, column=1).value)
        if current == field_name:
            worksheet.cell(row=row_number, column=2, value=value)
            return
    raise InputLayoutNormalizationError(
        "parametro_interno_ausente",
        f"Nao foi possivel localizar o parametro interno '{field_name}' no workbook canonico.",
        source=worksheet.title,
    )


def _write_employee_row(
    worksheet: Worksheet,
    row_number: int,
    *,
    employee_key: str | None,
    employee_name: str | None,
    domain_registration: str | None,
    status_colaborador: str,
    note: str | None,
    source_sheet: str,
    source_row: int,
) -> None:
    values = {
        "A": employee_key,
        "B": employee_name,
        "E": domain_registration,
        "H": status_colaborador,
        "I": "nao" if status_colaborador == "ignorar" else "sim",
        "J": note or f"layout={MONTHLY_LAYOUT_ID}; aba={source_sheet}; linha_origem={source_row}",
    }
    for column_letter, cell_value in values.items():
        worksheet[f"{column_letter}{row_number}"] = cell_value


def _write_launch_row(
    worksheet: Worksheet,
    row_number: int,
    *,
    event_column: str,
    value: str,
    employee_key: str | None,
    employee_name: str | None,
    domain_registration: str | None,
    observation: str,
) -> None:
    worksheet[f"B{row_number}"] = employee_key
    worksheet[f"C{row_number}"] = employee_name
    worksheet[f"D{row_number}"] = domain_registration
    worksheet[f"F{row_number}"] = observation
    worksheet[f"{event_column}{row_number}"] = value


def _build_launch_observation(
    *,
    inspection: InputWorkbookInspection,
    source_row: int,
    source_column: InputColumnMetadata,
    target_rubric: str,
    profile: CompanyColumnMappingProfile,
) -> str:
    return (
        f"layout={inspection.layout_id}; "
        f"aba={inspection.selected_sheet_name}; "
        f"linha_origem={source_row}; "
        f"coluna_origem={source_column.column_name}; "
        f"rubrica_target={target_rubric}; "
        f"profile_version={profile.profile_version}"
    )


def _infer_employee_status(row_values: dict[int, object]) -> tuple[str, str | None]:
    for column_index, value in row_values.items():
        if column_index in {1, 2}:
            continue
        text = normalized_optional_text(value)
        if text is None:
            continue
        normalized = _normalize_token(text)
        for token, status in STATUS_TOKENS.items():
            if token in normalized:
                return status, f"status_origem={token}"
    return "ativo", None


def _row_has_profile_data(
    row_values: dict[int, object],
    column_mappings: tuple[tuple[InputColumnMetadata, ColumnMappingRule], ...],
) -> bool:
    return any(not is_empty_value(row_values.get(column.column_index)) for column, _mapping in column_mappings)


def _row_is_empty(row_values: dict[int, object]) -> bool:
    return all(is_empty_value(value) for value in row_values.values())


def _normalize_token(value: object) -> str:
    text = unicodedata.normalize("NFKD", str(value)).strip().lower()
    text = "".join(character for character in text if not unicodedata.combining(character))
    cleaned = re.sub(r"[^a-z0-9]+", " ", text)
    return " ".join(cleaned.split())


__all__ = [
    "build_canonical_workbook_from_column_profile",
    "is_profile_identity_column",
    "normalize_workbook_with_column_profile",
]
