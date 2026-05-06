from __future__ import annotations

import json
from pathlib import Path

import pytest

from serialization import (
    SerializationEncodingError,
    SerializationSkipCode,
    default_serialization_summary_path,
    default_txt_output_path,
    deserialize_mapped_artifact,
    encode_mapped_movement_to_txt_line,
    render_serialized_txt,
    serialize_loaded_mapped_artifact,
    serialize_mapped_artifact_to_txt,
)


def _mapped_artifact_payload(*, movement_overrides: dict | None = None) -> dict:
    movement = {
        "canonical_movement_id": "mov-00001",
        "company_code": "72",
        "competence": "03/2024",
        "payroll_type": "mensal",
        "default_process": "11",
        "employee_key": "col-001",
        "employee_name": "Ana Lima",
        "event_name": "gratificacao",
        "value_type": "monetario",
        "quantity": None,
        "hours": None,
        "amount": "100",
        "source": {
            "sheet_name": "LANCAMENTOS_FACEIS",
            "row_number": 2,
            "cell": "H2",
            "column_name": "gratificacao",
        },
        "canonical_domain_registration": "123",
        "resolved_domain_registration": "123",
        "output_rubric": "201",
        "status": "pronto_para_serializer",
        "canonical_blocked": False,
        "inherited_pending_codes": [],
        "inherited_pending_messages": [],
        "mapping_pending_codes": [],
        "mapping_pending_messages": [],
        "observation": None,
        "informed_rubric": None,
        "event_nature": None,
        "serialization_unit": "BRL",
    }
    if movement_overrides:
        movement.update(movement_overrides)

    return {
        "artifact_version": "mapping_result_v1",
        "execution": {
            "engine_version": "0.1.0",
            "status": "success",
        },
        "snapshot": {
            "snapshot_version": "ingestion_snapshot_v1",
            "company_code": "72",
            "company_name": "Dela More",
            "competence": "03/2024",
            "layout_version": "v1",
            "movement_count": 1,
            "pending_count": 0,
            "execution_status": "success",
        },
        "config": {
            "company_code": "72",
            "company_name": "Dela More",
            "competence": "03/2024",
            "config_version": "2024.03.01",
            "default_process": "11",
            "active_event_mappings": 1,
            "active_employee_mappings": 1,
        },
        "mapped_movements": [movement],
        "mapping_pendings": [],
        "counts": {
            "mapped_movements": 1,
            "ready_movements": 1,
            "blocked_movements": 0,
            "mapping_pendings": 0,
            "blocking_mapping_pendings": 0,
        },
    }


def test_layout_43_line_is_generated_with_exact_width_and_padding():
    artifact = deserialize_mapped_artifact(_mapped_artifact_payload())

    line = encode_mapped_movement_to_txt_line(artifact.movements[0])

    assert len(line) == 43
    assert line == "1000000001232024030201110000100000000000072"


def test_serializer_encodes_hour_reference_without_separator():
    artifact = deserialize_mapped_artifact(
        _mapped_artifact_payload(
            movement_overrides={
                "event_name": "horas_extras_50",
                "value_type": "horas",
                "hours": {"text": "02:16", "total_minutes": 136},
                "amount": None,
                "output_rubric": "350",
                "serialization_unit": "HH:MM",
            }
        )
    )

    line = encode_mapped_movement_to_txt_line(artifact.movements[0])

    assert len(line) == 43
    assert line == "1000000001232024030350110000002160000000072"


def test_encoder_raises_when_numeric_field_exceeds_width():
    artifact = deserialize_mapped_artifact(
        _mapped_artifact_payload(
            movement_overrides={"resolved_domain_registration": "123456789012"}
        )
    )

    with pytest.raises(SerializationEncodingError, match="excede largura 11"):
        encode_mapped_movement_to_txt_line(artifact.movements[0])


def test_serializer_excludes_blocked_movement():
    artifact = deserialize_mapped_artifact(
        _mapped_artifact_payload(
            movement_overrides={"status": "bloqueado"}
        )
    )

    result = serialize_loaded_mapped_artifact(artifact)

    assert len(result.serialized_lines) == 0
    assert result.skipped_items[0].reason_code == SerializationSkipCode.MOVEMENT_NOT_READY


def test_serializer_excludes_movement_without_registration():
    artifact = deserialize_mapped_artifact(
        _mapped_artifact_payload(
            movement_overrides={"resolved_domain_registration": None}
        )
    )

    result = serialize_loaded_mapped_artifact(artifact)

    assert len(result.serialized_lines) == 0
    assert result.skipped_items[0].reason_code == SerializationSkipCode.MISSING_DOMAIN_REGISTRATION


def test_serializer_excludes_movement_without_output_rubric():
    artifact = deserialize_mapped_artifact(
        _mapped_artifact_payload(
            movement_overrides={"output_rubric": None}
        )
    )

    result = serialize_loaded_mapped_artifact(artifact)

    assert len(result.serialized_lines) == 0
    assert result.skipped_items[0].reason_code == SerializationSkipCode.MISSING_OUTPUT_RUBRIC


def test_serializer_persists_txt_and_summary_json(tmp_path: Path):
    mapped_path = tmp_path / "input.mapped.json"
    mapped_path.write_text(json.dumps(_mapped_artifact_payload()), encoding="utf-8")

    artifacts = serialize_mapped_artifact_to_txt(mapped_path)

    assert artifacts.txt_path == default_txt_output_path(mapped_path)
    assert artifacts.summary_path == default_serialization_summary_path(mapped_path)
    assert artifacts.txt_path.exists()
    assert artifacts.summary_path.exists()
    assert artifacts.txt_path.read_text(encoding="utf-8") == "1000000001232024030201110000100000000000072\n"

    payload = json.loads(artifacts.summary_path.read_text(encoding="utf-8"))
    assert payload["artifact_version"] == "serialization_summary_v1"
    assert payload["counts"]["serialized"] == 1
    assert payload["counts"]["non_serialized"] == 0
    assert payload["txt_path"].endswith(".txt")


def test_render_serialized_txt_returns_empty_string_when_nothing_is_serialized():
    artifact = deserialize_mapped_artifact(
        _mapped_artifact_payload(
            movement_overrides={"status": "bloqueado"}
        )
    )

    result = serialize_loaded_mapped_artifact(artifact)

    assert render_serialized_txt(result) == ""
