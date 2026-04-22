"""Operational dashboard helpers on top of the V1 pipeline."""

from .config_resolver import (
    DEFAULT_COMPANY_CONFIGS_ROOT,
    ConfigResolutionResult,
    ConfigResolutionStatus,
    ConfigResolver,
)
from .errors import DashboardOperationError
from .models import (
    DashboardActionRecord,
    DashboardActionType,
    DashboardConfigResolution,
    DashboardPaths,
    DashboardPendingItem,
    DashboardRunResult,
    DashboardState,
    DashboardSummary,
)
from .overrides import (
    apply_workbook_cell_correction,
    describe_ignore_strategy,
    ignore_pending_for_import,
    upsert_employee_mapping_override,
    upsert_event_mapping_override,
)
from .service import (
    build_dashboard_summary,
    collect_dashboard_pendings,
    is_txt_download_enabled,
    load_dashboard_run,
    run_dashboard_analysis,
)
from .storage import (
    DASHBOARD_SESSION_VERSION,
    DEFAULT_DASHBOARD_RUNS_ROOT,
    build_dashboard_paths,
    create_dashboard_run_from_paths,
    create_dashboard_run_from_uploads,
    load_dashboard_state,
    write_dashboard_state,
)

__all__ = [
    "ConfigResolutionResult",
    "ConfigResolutionStatus",
    "ConfigResolver",
    "DEFAULT_COMPANY_CONFIGS_ROOT",
    "DASHBOARD_SESSION_VERSION",
    "DEFAULT_DASHBOARD_RUNS_ROOT",
    "DashboardActionRecord",
    "DashboardActionType",
    "DashboardConfigResolution",
    "DashboardOperationError",
    "DashboardPaths",
    "DashboardPendingItem",
    "DashboardRunResult",
    "DashboardState",
    "DashboardSummary",
    "apply_workbook_cell_correction",
    "build_dashboard_paths",
    "build_dashboard_summary",
    "collect_dashboard_pendings",
    "create_dashboard_run_from_paths",
    "create_dashboard_run_from_uploads",
    "describe_ignore_strategy",
    "ignore_pending_for_import",
    "is_txt_download_enabled",
    "load_dashboard_run",
    "load_dashboard_state",
    "run_dashboard_analysis",
    "upsert_employee_mapping_override",
    "upsert_event_mapping_override",
    "write_dashboard_state",
]
