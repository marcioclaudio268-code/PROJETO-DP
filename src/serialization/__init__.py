"""Serialization package.

This package consumes the persisted mapped artifact, encodes eligible
movements into the 43-character fixed-width layout and persists both the TXT
and a JSON summary of the serialization run.
"""

from .artifact import (
    SUPPORTED_MAPPING_ARTIFACT_VERSION,
    deserialize_mapped_artifact,
    load_mapped_artifact,
)
from .encoder import (
    encode_mapped_movement_to_txt_line,
    evaluate_serialization_eligibility,
    render_serialized_txt,
    serialize_loaded_mapped_artifact,
)
from .errors import SerializationEncodingError, SerializationInputError
from .layout import LAYOUT_43_FIELDS, LAYOUT_43_TOTAL_WIDTH, LayoutFieldSpec, layout_43_field_names, layout_43_widths
from .models import (
    LoadedMappedArtifact,
    MappedArtifactMetadata,
    SerializableMappedMovement,
    SerializedTxtLine,
    SerializationResult,
    SerializationSkipCode,
    SerializationSkipItem,
)
from .persistence import (
    PersistedSerializationArtifacts,
    SERIALIZATION_SUMMARY_VERSION,
    default_serialization_summary_path,
    default_txt_output_path,
    infer_serialization_status,
    render_serialization_summary_json,
    serialize_serialization_summary,
    write_serialization_summary,
    write_serialized_txt,
)
from .pipeline import serialize_mapped_artifact_to_txt

__all__ = [
    "LAYOUT_43_FIELDS",
    "LAYOUT_43_TOTAL_WIDTH",
    "LayoutFieldSpec",
    "LoadedMappedArtifact",
    "MappedArtifactMetadata",
    "PersistedSerializationArtifacts",
    "SERIALIZATION_SUMMARY_VERSION",
    "SUPPORTED_MAPPING_ARTIFACT_VERSION",
    "SerializableMappedMovement",
    "SerializationEncodingError",
    "SerializationInputError",
    "SerializationResult",
    "SerializationSkipCode",
    "SerializationSkipItem",
    "SerializedTxtLine",
    "default_serialization_summary_path",
    "default_txt_output_path",
    "deserialize_mapped_artifact",
    "encode_mapped_movement_to_txt_line",
    "evaluate_serialization_eligibility",
    "infer_serialization_status",
    "layout_43_field_names",
    "layout_43_widths",
    "load_mapped_artifact",
    "render_serialization_summary_json",
    "render_serialized_txt",
    "serialize_loaded_mapped_artifact",
    "serialize_mapped_artifact_to_txt",
    "serialize_serialization_summary",
    "write_serialization_summary",
    "write_serialized_txt",
]
