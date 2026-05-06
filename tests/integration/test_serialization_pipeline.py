from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

from domain import (
    CanonicalMovement,
    IngestionResult,
    PayrollFileParameters,
    SourceRef,
    ValueType,
)
from ingestion import write_ingestion_snapshot
from mapping import map_snapshot_with_company_config
from serialization import serialize_mapped_artifact_to_txt


def _sample_ingestion_result() -> IngestionResult:
    parameters = PayrollFileParameters(
        company_code="72",
        company_name="Dela More",
        competence="03/2024",
        payroll_type="mensal",
        default_process="11",
        layout_version="v1",
        source_cells={"empresa_codigo": "B2"},
    )
    movements = (
        CanonicalMovement(
            movement_id="mov-00001",
            company_code="72",
            competence="03/2024",
            payroll_type="mensal",
            default_process="11",
            employee_key="col-001",
            employee_name="Ana Lima",
            domain_registration=None,
            event_name="gratificacao",
            value_type=ValueType.MONETARY,
            quantity=None,
            hours=None,
            amount=Decimal("100.00"),
            source=SourceRef("LANCAMENTOS_FACEIS", 2, "H2", "gratificacao"),
            blocked=False,
            serialization_unit="BRL",
        ),
    )
    return IngestionResult(
        parameters=parameters,
        employees=(),
        movements=movements,
        pendings=(),
    )


def test_full_pipeline_persists_txt_from_mapped_artifact(tmp_path: Path):
    snapshot_path = tmp_path / "input.snapshot.json"
    config_path = tmp_path / "company_config.json"

    write_ingestion_snapshot(_sample_ingestion_result(), snapshot_path, engine_version="0.1.0")
    config_path.write_text(
        json.dumps(
            {
                "company_code": "72",
                "company_name": "Dela More",
                "default_process": "11",
                "competence": "03/2024",
                "config_version": "2024.03.01",
                "event_mappings": [{"event_negocio": "gratificacao", "rubrica_saida": "201"}],
                "employee_mappings": [{"source_employee_key": "col-001", "domain_registration": "123"}],
                "pending_policy": {"block_on_unmapped_employee": True, "block_on_unmapped_event": True},
            }
        ),
        encoding="utf-8",
    )

    mapped_artifacts = map_snapshot_with_company_config(snapshot_path, config_path)
    serialization_artifacts = serialize_mapped_artifact_to_txt(mapped_artifacts.output_path)

    assert serialization_artifacts.txt_path.exists()
    assert serialization_artifacts.summary_path.exists()
    assert serialization_artifacts.txt_path.read_text(encoding="utf-8") == (
        "1000000001232024030201110000100000000000072\n"
    )

    summary_payload = json.loads(serialization_artifacts.summary_path.read_text(encoding="utf-8"))
    assert summary_payload["counts"]["serialized"] == 1
    assert summary_payload["counts"]["non_serialized"] == 0
    assert summary_payload["execution"]["status"] == "success"
