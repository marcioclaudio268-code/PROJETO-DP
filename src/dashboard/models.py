"""Typed models for the local operational dashboard layer."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class DashboardActionType(StrEnum):
    WORKBOOK_CELL_UPDATE = "corrigir_celula_planilha"
    EMPLOYEE_MAPPING_UPDATE = "corrigir_mapeamento_matricula"
    EVENT_MAPPING_UPDATE = "corrigir_rubrica_evento"
    IGNORE_PENDING = "ignorar_nesta_importacao"


class DashboardBaseModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class DashboardActionRecord(DashboardBaseModel):
    action_id: str
    action_type: DashboardActionType
    description: str
    pending_uid: str | None = None
    applied_at: datetime = Field(default_factory=_utc_now)
    payload: dict[str, Any] = Field(default_factory=dict)


class DashboardState(DashboardBaseModel):
    session_version: str
    source_workbook_name: str
    source_config_name: str | None = None
    actions: list[DashboardActionRecord] = Field(default_factory=list)
    last_analysis: dict[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class DashboardPaths:
    run_root: Path
    inputs_dir: Path
    artifacts_dir: Path
    state_path: Path
    raw_workbook_path: Path
    editable_workbook_path: Path
    editable_config_path: Path
    analyzed_workbook_path: Path
    snapshot_path: Path
    manifest_path: Path
    normalization_path: Path
    mapped_artifact_path: Path
    txt_path: Path
    serialization_summary_path: Path
    validation_path: Path


@dataclass(frozen=True, slots=True)
class DashboardPendingItem:
    uid: str
    stage: str
    pending_id: str
    code: str
    severity: str
    employee_name: str | None
    employee_key: str | None
    event_name: str | None
    field_label: str
    found_value: str | None
    problem: str
    recommended_action: str
    source_sheet: str | None
    source_cell: str | None
    source_row: int | None
    source_column_name: str | None
    can_edit_workbook: bool
    can_edit_employee_mapping: bool
    can_edit_event_mapping: bool
    can_ignore: bool
    ignore_mode: str | None = None
    ignore_label: str | None = None

    def selection_label(self) -> str:
        employee = self.employee_name or self.employee_key or "Item sem colaborador"
        return f"[{self.stage}] {employee} | {self.field_label} | {self.problem}"

    def table_row(self) -> dict[str, str]:
        actions: list[str] = []
        if self.can_edit_workbook:
            actions.append("corrigir valor")
        if self.can_edit_employee_mapping:
            actions.append("corrigir matricula")
        if self.can_edit_event_mapping:
            actions.append("corrigir rubrica")
        if self.can_ignore:
            actions.append("ignorar nesta importacao")

        return {
            "Funcionario": self.employee_name or self.employee_key or "-",
            "Campo": self.field_label,
            "Valor encontrado": self.found_value or "-",
            "Problema": self.problem,
            "O que fazer": self.recommended_action,
            "Acao": ", ".join(actions) if actions else "somente leitura",
        }


@dataclass(frozen=True, slots=True)
class DashboardSummary:
    company_name: str
    company_code: str
    competence: str
    employee_count: int
    relevant_movement_count: int
    pending_count: int
    ignored_count: int
    serialized_line_count: int
    validation_status: str
    status_label: str
    recommendation: str
    txt_enabled: bool
    txt_status_label: str
    config_status: str
    config_status_label: str
    config_source: str | None
    config_version: str | None


@dataclass(frozen=True, slots=True)
class DashboardConfigResolution:
    status: str
    status_label: str
    message: str
    company_code: str
    competence: str
    config_source: str | None
    config_version: str | None
    source_path: str | None


@dataclass(frozen=True, slots=True)
class DashboardRunResult:
    paths: DashboardPaths
    state: DashboardState
    summary: DashboardSummary
    config_resolution: DashboardConfigResolution
    pendings: tuple[DashboardPendingItem, ...]
    snapshot_payload: dict[str, Any]
    mapped_payload: dict[str, Any]
    serialization_payload: dict[str, Any]
    validation_payload: dict[str, Any]
