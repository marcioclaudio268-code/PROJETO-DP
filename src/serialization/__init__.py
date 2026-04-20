"""Serialization package.

Fixed-width TXT layout metadata and, later, the serializer implementation.
"""

from .layout import LAYOUT_43_FIELDS, LAYOUT_43_TOTAL_WIDTH, LayoutFieldSpec, layout_43_field_names, layout_43_widths

__all__ = [
    "LAYOUT_43_FIELDS",
    "LAYOUT_43_TOTAL_WIDTH",
    "LayoutFieldSpec",
    "layout_43_field_names",
    "layout_43_widths",
]
