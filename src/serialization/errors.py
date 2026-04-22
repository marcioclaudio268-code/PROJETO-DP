"""Exceptions for fixed-width TXT serialization."""

from __future__ import annotations


class SerializationInputError(ValueError):
    """Raised when the mapped artifact cannot be consumed safely."""

    def __init__(self, code: str, message: str, source: str | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.source = source


class SerializationEncodingError(ValueError):
    """Raised when one eligible movement cannot be encoded safely."""

    def __init__(self, code: str, message: str, field_name: str | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.field_name = field_name

