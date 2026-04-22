"""Dashboard-layer operational errors."""

from __future__ import annotations


class DashboardOperationError(RuntimeError):
    """Raised when a guided dashboard action cannot be applied safely."""

    def __init__(self, code: str, message: str, *, source: str | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.source = source
