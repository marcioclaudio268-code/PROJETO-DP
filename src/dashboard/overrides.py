"""Guided overrides applied on top of the editable dashboard workspace."""

from __future__ import annotations

import json
from pathlib import Path
from uuid import uuid4

from openpyxl import load_workbook

from config import CompanyConfig
from ingestion.template_v1 import LANCAMENTOS_FACEIS_HEADERS
from ingestion.template_v1_loader import EVENT_SPECS

from .errors import DashboardOperationError
from .models import DashboardActionRecord, DashboardActionType, DashboardPaths, DashboardPendingItem
from .storage import load_dashboard_state, write_dashboard_state


EVENT_COLUMN_NAMES = tuple(spec.column_name for spec in EVENT_SPECS)
IGNORABLE_NOTE_CODES = {"observacao_ambigua"}
IGNORABLE_EVENT_CODES = {
    "evento_nao_automatizavel",
    "hora_invalida",
    "valor_invalido",
    "quantidade_invalida",
    "mapeamento_evento_ausente",
    "mapeamento_evento_inativo",
    "evento_requer_revisao_por_politica",
}
IGNORABLE_LINE_CODES = {
    "funcionario_nao_encontrado",
    "matricula_dominio_ausente",
    "matricula_dominio_informada_somente_na_linha",
    "matricula_dominio_divergente",
    "linha_bloqueada_por_status",
    "mapeamento_matricula_ausente",
    "mapeamento_matricula_divergente_config",
    "mapeamento_matricula_ambiguo",
}


def apply_workbook_cell_correction(
    paths: DashboardPaths,
    *,
    sheet_name: str,
    cell: str,
    new_value: str | None,
    pending_uid: str | None = None,
    description: str | None = None,
) -> DashboardActionRecord:
    workbook = load_workbook(paths.editable_workbook_path)
    if sheet_name not in workbook.sheetnames:
        raise DashboardOperationError(
            "aba_para_correcao_ausente",
            f"Aba '{sheet_name}' nao encontrada na planilha editavel.",
            source=sheet_name,
        )

    worksheet = workbook[sheet_name]
    original_value = worksheet[cell].value
    worksheet[cell] = None if new_value in {"", None} else new_value
    workbook.save(paths.editable_workbook_path)

    action = DashboardActionRecord(
        action_id=f"act-{uuid4().hex[:10]}",
        action_type=DashboardActionType.WORKBOOK_CELL_UPDATE,
        pending_uid=pending_uid,
        description=description or f"Correcao aplicada em {sheet_name}!{cell}.",
        payload={
            "sheet_name": sheet_name,
            "cell": cell,
            "original_value": _stringify(original_value),
            "new_value": _stringify(worksheet[cell].value),
        },
    )
    _append_action(paths, action)
    return action


def upsert_employee_mapping_override(
    paths: DashboardPaths,
    *,
    employee_key: str,
    domain_registration: str,
    employee_name: str | None = None,
    pending_uid: str | None = None,
) -> DashboardActionRecord:
    payload = _load_company_config_payload(paths.editable_config_path)
    mappings = list(payload.get("employee_mappings", []))

    updated = False
    for item in mappings:
        if item.get("source_employee_key") == employee_key:
            item["domain_registration"] = domain_registration
            if employee_name:
                item["source_employee_name"] = employee_name
            item["active"] = True
            updated = True
            break

    if not updated:
        mappings.append(
            {
                "source_employee_key": employee_key,
                "source_employee_name": employee_name,
                "domain_registration": domain_registration,
                "active": True,
                "aliases": [],
                "notes": None,
            }
        )

    payload["employee_mappings"] = mappings
    _write_validated_company_config(paths.editable_config_path, payload)

    action = DashboardActionRecord(
        action_id=f"act-{uuid4().hex[:10]}",
        action_type=DashboardActionType.EMPLOYEE_MAPPING_UPDATE,
        pending_uid=pending_uid,
        description=f"Correcao de matricula aplicada para a chave '{employee_key}'.",
        payload={
            "employee_key": employee_key,
            "domain_registration": domain_registration,
            "employee_name": employee_name,
        },
    )
    _append_action(paths, action)
    return action


def upsert_event_mapping_override(
    paths: DashboardPaths,
    *,
    event_name: str,
    output_rubric: str,
    pending_uid: str | None = None,
) -> DashboardActionRecord:
    payload = _load_company_config_payload(paths.editable_config_path)
    mappings = list(payload.get("event_mappings", []))

    updated = False
    for item in mappings:
        if item.get("event_negocio") == event_name:
            item["rubrica_saida"] = output_rubric
            item["active"] = True
            updated = True
            break

    if not updated:
        mappings.append(
            {
                "event_negocio": event_name,
                "rubrica_saida": output_rubric,
                "active": True,
                "notes": None,
            }
        )

    payload["event_mappings"] = mappings
    _write_validated_company_config(paths.editable_config_path, payload)

    action = DashboardActionRecord(
        action_id=f"act-{uuid4().hex[:10]}",
        action_type=DashboardActionType.EVENT_MAPPING_UPDATE,
        pending_uid=pending_uid,
        description=f"Correcao de rubrica aplicada para o evento '{event_name}'.",
        payload={
            "event_name": event_name,
            "output_rubric": output_rubric,
        },
    )
    _append_action(paths, action)
    return action


def ignore_pending_for_import(
    paths: DashboardPaths,
    pending_item: DashboardPendingItem,
) -> DashboardActionRecord:
    if not pending_item.can_ignore or pending_item.ignore_mode is None:
        raise DashboardOperationError(
            "ignorar_nao_permitido",
            "Este item nao pode ser ignorado com seguranca nesta importacao.",
            source=pending_item.uid,
        )

    if pending_item.source_sheet != "LANCAMENTOS_FACEIS":
        raise DashboardOperationError(
            "ignorar_sem_apoio_operacional",
            "Somente itens originados em LANCAMENTOS_FACEIS podem ser ignorados neste MVP.",
            source=pending_item.uid,
        )

    workbook = load_workbook(paths.editable_workbook_path)
    worksheet = workbook[pending_item.source_sheet]

    if pending_item.ignore_mode == "observacao":
        cells_to_clear = (pending_item.source_cell,)
    elif pending_item.ignore_mode == "evento":
        cells_to_clear = (pending_item.source_cell,)
    elif pending_item.ignore_mode == "linha":
        if pending_item.source_row is None:
            raise DashboardOperationError(
                "linha_para_ignorar_ausente",
                "Nao foi possivel identificar a linha a ser ignorada nesta importacao.",
                source=pending_item.uid,
            )
        cells_to_clear = _event_cells_for_row(worksheet, pending_item.source_row)
    else:
        raise DashboardOperationError(
            "modo_ignorar_nao_suportado",
            f"Modo de ignorar nao suportado: {pending_item.ignore_mode}.",
            source=pending_item.uid,
        )

    original_values: dict[str, str | None] = {}
    for cell in cells_to_clear:
        if cell is None:
            continue
        original_values[cell] = _stringify(worksheet[cell].value)
        worksheet[cell] = None

    workbook.save(paths.editable_workbook_path)

    action = DashboardActionRecord(
        action_id=f"act-{uuid4().hex[:10]}",
        action_type=DashboardActionType.IGNORE_PENDING,
        pending_uid=pending_item.uid,
        description=pending_item.ignore_label or "Item ignorado nesta importacao.",
        payload={
            "ignore_mode": pending_item.ignore_mode,
            "cleared_cells": [cell for cell in cells_to_clear if cell is not None],
            "original_values": original_values,
            "pending_code": pending_item.code,
        },
    )
    _append_action(paths, action)
    return action


def describe_ignore_strategy(
    *,
    code: str,
    source_sheet: str | None,
    source_cell: str | None,
    source_row: int | None,
    event_name: str | None,
) -> tuple[bool, str | None, str | None]:
    if source_sheet != "LANCAMENTOS_FACEIS":
        return False, None, None

    if code in IGNORABLE_NOTE_CODES and source_cell is not None:
        return True, "observacao", "Ignorar observacao nesta importacao"

    if code in IGNORABLE_EVENT_CODES and source_cell is not None and event_name is not None:
        return True, "evento", "Ignorar este evento nesta importacao"

    if code in IGNORABLE_LINE_CODES and source_row is not None:
        return True, "linha", "Ignorar esta linha nesta importacao"

    return False, None, None


def _event_cells_for_row(worksheet, row_number: int) -> tuple[str | None, ...]:
    header_map = {
        str(worksheet.cell(row=1, column=column_index).value): column_index
        for column_index in range(1, worksheet.max_column + 1)
        if worksheet.cell(row=1, column=column_index).value is not None
    }

    cells: list[str] = []
    for header_name in (*EVENT_COLUMN_NAMES, "observacao_eventos"):
        column_index = header_map.get(header_name)
        if column_index is None:
            continue
        cells.append(f"{_column_letter(column_index)}{row_number}")
    return tuple(cells)


def _column_letter(column_index: int) -> str:
    quotient = column_index
    letters = ""
    while quotient:
        quotient, remainder = divmod(quotient - 1, 26)
        letters = chr(65 + remainder) + letters
    return letters


def _append_action(paths: DashboardPaths, action: DashboardActionRecord) -> None:
    state = load_dashboard_state(paths.state_path)
    updated_actions = [*state.actions, action]
    updated_state = state.model_copy(update={"actions": updated_actions})
    write_dashboard_state(paths.state_path, updated_state)


def _load_company_config_payload(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise DashboardOperationError(
            "config_empresa_invalida_no_dashboard",
            "A configuracao da empresa esta invalida para edicao guiada.",
            source=str(path),
        ) from exc


def _write_validated_company_config(path: Path, payload: dict) -> None:
    config = CompanyConfig.model_validate(payload)
    path.write_text(
        json.dumps(config.model_dump(mode="json"), ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _stringify(value) -> str | None:
    if value is None:
        return None
    return str(value)
