"""Operational pipeline for fixed-width TXT serialization."""

from __future__ import annotations

from pathlib import Path

from .artifact import load_mapped_artifact
from .encoder import serialize_loaded_mapped_artifact
from .persistence import (
    PersistedSerializationArtifacts,
    default_serialization_summary_path,
    default_txt_output_path,
    write_serialization_summary,
    write_serialized_txt,
)


def serialize_mapped_artifact_to_txt(
    mapped_artifact_path: str | Path,
    *,
    txt_path: str | Path | None = None,
    summary_path: str | Path | None = None,
) -> PersistedSerializationArtifacts:
    input_path = Path(mapped_artifact_path)
    artifact = load_mapped_artifact(input_path)
    result = serialize_loaded_mapped_artifact(artifact)

    txt_target = Path(txt_path) if txt_path is not None else default_txt_output_path(input_path)
    write_serialized_txt(result, txt_target)

    summary_target = (
        Path(summary_path)
        if summary_path is not None
        else default_serialization_summary_path(input_path)
    )
    write_serialization_summary(
        result,
        summary_target,
        mapped_artifact_path=input_path,
        txt_path=txt_target,
    )

    return PersistedSerializationArtifacts(
        result=result,
        mapped_artifact_path=input_path,
        txt_path=txt_target,
        summary_path=summary_target,
    )

