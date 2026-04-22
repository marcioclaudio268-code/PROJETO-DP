"""Workspace and state persistence for the local dashboard."""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from uuid import uuid4

from .models import DashboardPaths, DashboardState


DASHBOARD_SESSION_VERSION = "dashboard_session_v1"
DEFAULT_DASHBOARD_RUNS_ROOT = Path("data/runs/dashboard_v1")


def build_dashboard_paths(run_root: str | Path) -> DashboardPaths:
    root = Path(run_root)
    inputs_dir = root / "inputs"
    artifacts_dir = root / "artifacts"

    return DashboardPaths(
        run_root=root,
        inputs_dir=inputs_dir,
        artifacts_dir=artifacts_dir,
        state_path=root / "dashboard_state.json",
        editable_workbook_path=inputs_dir / "input.xlsx",
        editable_config_path=inputs_dir / "company_config.json",
        analyzed_workbook_path=artifacts_dir / "analyzed_workbook.xlsx",
        snapshot_path=artifacts_dir / "input.snapshot.json",
        manifest_path=artifacts_dir / "input.manifest.json",
        mapped_artifact_path=artifacts_dir / "input.mapped.json",
        txt_path=artifacts_dir / "input.txt",
        serialization_summary_path=artifacts_dir / "input.serialization.json",
        validation_path=artifacts_dir / "input.validation.json",
    )


def create_dashboard_run_from_paths(
    workbook_path: str | Path,
    config_path: str | Path | None = None,
    *,
    runs_root: str | Path | None = None,
    run_id: str | None = None,
) -> DashboardPaths:
    source_workbook = Path(workbook_path)
    target_paths = _prepare_run_root(runs_root=runs_root, run_id=run_id)

    shutil.copy2(source_workbook, target_paths.editable_workbook_path)
    source_config_name = None
    if config_path is not None:
        source_config = Path(config_path)
        shutil.copy2(source_config, target_paths.editable_config_path)
        source_config_name = source_config.name

    state = DashboardState(
        session_version=DASHBOARD_SESSION_VERSION,
        source_workbook_name=source_workbook.name,
        source_config_name=source_config_name,
    )
    write_dashboard_state(target_paths.state_path, state)
    return target_paths


def create_dashboard_run_from_uploads(
    *,
    workbook_name: str,
    workbook_bytes: bytes,
    runs_root: str | Path | None = None,
    run_id: str | None = None,
) -> DashboardPaths:
    target_paths = _prepare_run_root(runs_root=runs_root, run_id=run_id)

    target_paths.editable_workbook_path.write_bytes(workbook_bytes)

    state = DashboardState(
        session_version=DASHBOARD_SESSION_VERSION,
        source_workbook_name=workbook_name,
        source_config_name=None,
    )
    write_dashboard_state(target_paths.state_path, state)
    return target_paths


def load_dashboard_state(path: str | Path) -> DashboardState:
    state_path = Path(path)
    if not state_path.exists():
        raise FileNotFoundError(f"Estado do dashboard nao encontrado: {state_path}")
    return DashboardState.model_validate_json(state_path.read_text(encoding="utf-8"))


def write_dashboard_state(path: str | Path, state: DashboardState) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(state.model_dump(mode="json"), ensure_ascii=True, indent=2, sort_keys=True) + "\n"
    target.write_text(payload, encoding="utf-8")
    return target


def _prepare_run_root(
    *,
    runs_root: str | Path | None,
    run_id: str | None,
) -> DashboardPaths:
    root = Path(runs_root) if runs_root is not None else DEFAULT_DASHBOARD_RUNS_ROOT
    root.mkdir(parents=True, exist_ok=True)
    run_name = run_id or f"run-{uuid4().hex[:10]}"
    run_root = root / run_name
    run_root.mkdir(parents=True, exist_ok=False)
    paths = build_dashboard_paths(run_root)
    paths.inputs_dir.mkdir(parents=True, exist_ok=True)
    paths.artifacts_dir.mkdir(parents=True, exist_ok=True)
    return paths
