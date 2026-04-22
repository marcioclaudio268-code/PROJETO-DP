"""Operational orchestration for the guided local dashboard."""

from __future__ import annotations

import json
from pathlib import Path

from openpyxl import load_workbook

from mapping.pipeline import map_snapshot_with_company_config
from serialization.pipeline import serialize_mapped_artifact_to_txt
from validation.pipeline import validate_pipeline_v1

from ingestion.pipeline import ingest_fill_and_persist_planilha_padrao_v1

from .models import DashboardPaths, DashboardPendingItem, DashboardRunResult, DashboardSummary
from .overrides import describe_ignore_strategy
from .storage import load_dashboard_state, write_dashboard_state


CONFIG_EDIT_EMPLOYEE_CODES = {
    "mapeamento_matricula_ausente",
    "mapeamento_matricula_divergente_config",
}
CONFIG_EDIT_EVENT_CODES = {
    "mapeamento_evento_ausente",
    "mapeamento_evento_inativo",
}


def run_dashboard_analysis(paths: DashboardPaths) -> DashboardRunResult:
    ingest_fill_and_persist_planilha_padrao_v1(
        paths.editable_workbook_path,
        output_path=paths.analyzed_workbook_path,
        snapshot_path=paths.snapshot_path,
        manifest_path=paths.manifest_path,
        write_manifest_file=True,
    )
    map_snapshot_with_company_config(
        paths.snapshot_path,
        paths.editable_config_path,
        output_path=paths.mapped_artifact_path,
    )
    serialize_mapped_artifact_to_txt(
        paths.mapped_artifact_path,
        txt_path=paths.txt_path,
        summary_path=paths.serialization_summary_path,
    )
    validate_pipeline_v1(
        snapshot_path=paths.snapshot_path,
        mapped_artifact_path=paths.mapped_artifact_path,
        txt_path=paths.txt_path,
        serialization_summary_path=paths.serialization_summary_path,
        output_path=paths.validation_path,
    )

    snapshot_payload = _load_json(paths.snapshot_path)
    mapped_payload = _load_json(paths.mapped_artifact_path)
    serialization_payload = _load_json(paths.serialization_summary_path)
    validation_payload = _load_json(paths.validation_path)
    state = load_dashboard_state(paths.state_path)
    pendings = collect_dashboard_pendings(
        paths=paths,
        snapshot_payload=snapshot_payload,
        mapped_payload=mapped_payload,
    )
    summary = build_dashboard_summary(
        state=state,
        snapshot_payload=snapshot_payload,
        serialization_payload=serialization_payload,
        validation_payload=validation_payload,
        pending_count=len(pendings),
    )

    updated_state = state.model_copy(
        update={
            "last_analysis": {
                "company_name": summary.company_name,
                "competence": summary.competence,
                "validation_status": summary.validation_status,
                "txt_enabled": summary.txt_enabled,
                "pending_count": summary.pending_count,
                "serialized_line_count": summary.serialized_line_count,
            }
        }
    )
    write_dashboard_state(paths.state_path, updated_state)

    return DashboardRunResult(
        paths=paths,
        state=updated_state,
        summary=summary,
        pendings=pendings,
        snapshot_payload=snapshot_payload,
        mapped_payload=mapped_payload,
        serialization_payload=serialization_payload,
        validation_payload=validation_payload,
    )


def load_dashboard_run(paths: DashboardPaths) -> DashboardRunResult:
    snapshot_payload = _load_json(paths.snapshot_path)
    mapped_payload = _load_json(paths.mapped_artifact_path)
    serialization_payload = _load_json(paths.serialization_summary_path)
    validation_payload = _load_json(paths.validation_path)
    state = load_dashboard_state(paths.state_path)
    pendings = collect_dashboard_pendings(
        paths=paths,
        snapshot_payload=snapshot_payload,
        mapped_payload=mapped_payload,
    )
    summary = build_dashboard_summary(
        state=state,
        snapshot_payload=snapshot_payload,
        serialization_payload=serialization_payload,
        validation_payload=validation_payload,
        pending_count=len(pendings),
    )
    return DashboardRunResult(
        paths=paths,
        state=state,
        summary=summary,
        pendings=pendings,
        snapshot_payload=snapshot_payload,
        mapped_payload=mapped_payload,
        serialization_payload=serialization_payload,
        validation_payload=validation_payload,
    )


def build_dashboard_summary(
    *,
    state,
    snapshot_payload: dict,
    serialization_payload: dict,
    validation_payload: dict,
    pending_count: int,
) -> DashboardSummary:
    parameters = snapshot_payload["parameters"]
    counts = snapshot_payload["counts"]
    validation_status = str(validation_payload["execution"]["status"])
    txt_enabled = is_txt_download_enabled(
        validation_payload=validation_payload,
        serialization_payload=serialization_payload,
    )
    serialized_line_count = int(serialization_payload["counts"]["serialized"])
    ignored_count = sum(
        1 for action in state.actions if action.action_type.value == "ignorar_nesta_importacao"
    )

    return DashboardSummary(
        company_name=str(parameters["company_name"]),
        company_code=str(parameters["company_code"]),
        competence=str(parameters["competence"]),
        employee_count=int(counts["employees"]),
        relevant_movement_count=int(counts["movements"]),
        pending_count=pending_count,
        ignored_count=ignored_count,
        serialized_line_count=serialized_line_count,
        validation_status=validation_status,
        status_label=_humanize_validation_status(validation_status),
        recommendation=str(validation_payload["recommendation"]),
        txt_enabled=txt_enabled,
        txt_status_label="Liberado para baixar" if txt_enabled else "Ainda bloqueado",
    )


def is_txt_download_enabled(*, validation_payload: dict, serialization_payload: dict) -> bool:
    validation_status = str(validation_payload["execution"]["status"])
    serialized_count = int(serialization_payload["counts"]["serialized"])
    has_blocking_issues = bool(validation_payload.get("fatal_errors")) or bool(
        validation_payload.get("inconsistencies")
    )
    return (
        validation_status in {"success", "success_with_warnings"}
        and not has_blocking_issues
        and serialized_count > 0
    )


def collect_dashboard_pendings(
    *,
    paths: DashboardPaths,
    snapshot_payload: dict,
    mapped_payload: dict,
) -> tuple[DashboardPendingItem, ...]:
    workbook = load_workbook(paths.editable_workbook_path, data_only=False)
    config_payload = _load_json(paths.editable_config_path)

    pendings: list[DashboardPendingItem] = []
    for pending_payload in snapshot_payload.get("pendings", ()):
        pendings.append(
            _build_pending_item(
                stage="ingestao",
                pending_payload=pending_payload,
                workbook=workbook,
                config_payload=config_payload,
            )
        )

    for pending_payload in mapped_payload.get("mapping_pendings", ()):
        pendings.append(
            _build_pending_item(
                stage="mapeamento",
                pending_payload=pending_payload,
                workbook=workbook,
                config_payload=config_payload,
            )
        )

    return tuple(pendings)


def _build_pending_item(
    *,
    stage: str,
    pending_payload: dict,
    workbook,
    config_payload: dict,
) -> DashboardPendingItem:
    source = pending_payload.get("source", {})
    code = str(pending_payload["pending_code"])
    event_name = pending_payload.get("event_name")
    source_sheet = source.get("sheet_name")
    source_cell = source.get("cell")
    source_row = int(source["row_number"]) if source.get("row_number") is not None else None
    source_column_name = source.get("column_name")
    can_edit_employee_mapping = code in CONFIG_EDIT_EMPLOYEE_CODES
    can_edit_event_mapping = code in CONFIG_EDIT_EVENT_CODES
    can_edit_workbook = bool(source_sheet and source_cell) and not (
        can_edit_employee_mapping or can_edit_event_mapping
    )
    can_ignore, ignore_mode, ignore_label = describe_ignore_strategy(
        code=code,
        source_sheet=source_sheet,
        source_cell=source_cell,
        source_row=source_row,
        event_name=event_name,
    )

    return DashboardPendingItem(
        uid=f"{stage}:{pending_payload['pending_id']}",
        stage=stage,
        pending_id=str(pending_payload["pending_id"]),
        code=code,
        severity=str(pending_payload["severity"]),
        employee_name=pending_payload.get("employee_name"),
        employee_key=pending_payload.get("employee_key"),
        event_name=event_name,
        field_label=_resolve_field_label(code=code, source_column_name=source_column_name),
        found_value=_resolve_found_value(
            code=code,
            source_sheet=source_sheet,
            source_cell=source_cell,
            workbook=workbook,
            config_payload=config_payload,
            employee_key=pending_payload.get("employee_key"),
            event_name=event_name,
        ),
        problem=str(pending_payload["description"]),
        recommended_action=str(pending_payload["recommended_action"]),
        source_sheet=source_sheet,
        source_cell=source_cell,
        source_row=source_row,
        source_column_name=source_column_name,
        can_edit_workbook=can_edit_workbook,
        can_edit_employee_mapping=can_edit_employee_mapping,
        can_edit_event_mapping=can_edit_event_mapping,
        can_ignore=can_ignore,
        ignore_mode=ignore_mode,
        ignore_label=ignore_label,
    )


def _resolve_field_label(*, code: str, source_column_name: str | None) -> str:
    if code in CONFIG_EDIT_EMPLOYEE_CODES:
        return "matricula do cadastro da empresa"
    if code in CONFIG_EDIT_EVENT_CODES:
        return "rubrica de saida"
    return source_column_name or "campo da planilha"


def _resolve_found_value(
    *,
    code: str,
    source_sheet: str | None,
    source_cell: str | None,
    workbook,
    config_payload: dict,
    employee_key: str | None,
    event_name: str | None,
) -> str | None:
    if code in CONFIG_EDIT_EMPLOYEE_CODES:
        for item in config_payload.get("employee_mappings", []):
            if item.get("source_employee_key") == employee_key:
                return _stringify(item.get("domain_registration")) or "em branco"
        return "sem cadastro"

    if code in CONFIG_EDIT_EVENT_CODES:
        for item in config_payload.get("event_mappings", []):
            if item.get("event_negocio") == event_name:
                return _stringify(item.get("rubrica_saida")) or "em branco"
        return "sem rubrica"

    if source_sheet and source_sheet in workbook.sheetnames and source_cell is not None:
        return _stringify(workbook[source_sheet][source_cell].value) or "em branco"

    return None


def _humanize_validation_status(status: str) -> str:
    if status == "success":
        return "Analise concluida e pronta para exportacao"
    if status == "success_with_warnings":
        return "Analise concluida com alertas"
    if status == "empty":
        return "Sem linhas liberadas para exportacao"
    return "Analise bloqueada"


def _load_json(path: str | Path) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _stringify(value) -> str | None:
    if value is None:
        return None
    return str(value)
