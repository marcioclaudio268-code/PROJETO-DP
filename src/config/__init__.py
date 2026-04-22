"""Configuration package.

Pydantic models for company config, mappings, pending policy, run manifest and master data.
"""

from .master_data import (
    DEFAULT_LEGACY_COMPANY_CONFIGS_ROOT,
    DEFAULT_MASTER_DATA_ROOT,
    CompanyMasterDataStore,
    MasterDataImportError,
    build_master_data_paths,
    import_resumo_mensal_file,
)
from .models import (
    CompanyConfig,
    CompanyConfigIssue,
    CompanyConfigRecord,
    CompanyRegistryEntry,
    EmployeeMapping,
    EventMapping,
    MasterDataImportResult,
    PendingPolicy,
    RunManifest,
)

__all__ = [
    "DEFAULT_LEGACY_COMPANY_CONFIGS_ROOT",
    "DEFAULT_MASTER_DATA_ROOT",
    "CompanyConfigIssue",
    "CompanyConfig",
    "CompanyConfigRecord",
    "CompanyMasterDataStore",
    "CompanyRegistryEntry",
    "EmployeeMapping",
    "EventMapping",
    "MasterDataImportError",
    "MasterDataImportResult",
    "PendingPolicy",
    "RunManifest",
    "build_master_data_paths",
    "import_resumo_mensal_file",
]
