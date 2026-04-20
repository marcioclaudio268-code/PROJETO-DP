"""Domain package.

Pure domain models and invariants for the payroll TXT engine live here.
No IO, no company hardcode, no serializer logic.
"""

from .canonical import (
    CanonicalMovement,
    IngestionResult,
    NormalizedHours,
    PayrollFileParameters,
    PendingCode,
    PendingItem,
    PendingSeverity,
    RegistrationSource,
    ResolvedEmployee,
    SourceRef,
    ValueType,
    decimal_to_plain_string,
)

__all__ = [
    "CanonicalMovement",
    "IngestionResult",
    "NormalizedHours",
    "PayrollFileParameters",
    "PendingCode",
    "PendingItem",
    "PendingSeverity",
    "RegistrationSource",
    "ResolvedEmployee",
    "SourceRef",
    "ValueType",
    "decimal_to_plain_string",
]
