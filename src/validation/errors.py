"""Exceptions for final validation and reconciliation."""

from __future__ import annotations


class FinalValidationInputError(ValueError):
    """Raised when one validation input artifact cannot be consumed safely."""

    def __init__(self, code: str, message: str, source: str | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.source = source

