"""Configuration package.

Pydantic models for company config, mappings, pending policy, run manifest and master data.
"""

from .master_data import (
    DEFAULT_LEGACY_COMPANY_CONFIGS_ROOT,
    DEFAULT_MASTER_DATA_ROOT,
    CompanyMasterDataStore,
    MasterDataImportError,
    seed_company_configs_from_missing_issues,
    build_master_data_paths,
    import_resumo_mensal_file,
)
from .models import (
    CompanyConfig,
    CompanyConfigIssue,
    CompanyConfigSeedException,
    CompanyConfigSeedGroupResult,
    CompanyConfigRecord,
    CompanyRegistryEntry,
    EmployeeMapping,
    EventMapping,
    MasterDataImportResult,
    MasterDataSeedResult,
    PendingPolicy,
    RunManifest,
)

__all__ = [
    "DEFAULT_LEGACY_COMPANY_CONFIGS_ROOT",
    "DEFAULT_MASTER_DATA_ROOT",
    "CompanyConfigIssue",
    "CompanyConfig",
    "CompanyConfigSeedException",
    "CompanyConfigSeedGroupResult",
    "CompanyConfigRecord",
    "CompanyMasterDataStore",
    "CompanyRegistryEntry",
    "EmployeeMapping",
    "EventMapping",
    "MasterDataImportError",
    "MasterDataImportResult",
    "MasterDataSeedResult",
    "PendingPolicy",
    "RunManifest",
    "build_master_data_paths",
    "import_resumo_mensal_file",
    "seed_company_configs_from_missing_issues",
]
