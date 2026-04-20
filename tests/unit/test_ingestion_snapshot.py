from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal

from domain import (
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
)
from ingestion import build_ingestion_manifest, render_ingestion_snapshot_json, serialize_ingestion_result


def _sample_result() -> IngestionResult:
    parameters = PayrollFileParameters(
        company_code="72",
        company_name="Dela More",
        competence="03/2024",
        payroll_type="mensal",
        default_process="11",
        layout_version="v1",
        source_cells={"empresa_codigo": "B2"},
    )
    employees = (
        ResolvedEmployee(
            employee_key="col-001",
            employee_name="Ana Lima",
            domain_registration="123",
            status="ativo",
            allows_entries=True,
            source=SourceRef("FUNCIONARIOS", 2, "A2", "chave_colaborador"),
            resolved_from_registry=True,
            registration_source=RegistrationSource.REGISTRY,
            registry_consistent=True,
        ),
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
            domain_registration="123",
            event_name="gratificacao",
            value_type=ValueType.MONETARY,
            quantity=None,
            hours=None,
            amount=Decimal("100.25"),
            source=SourceRef("LANCAMENTOS_FACEIS", 2, "H2", "gratificacao"),
            blocked=False,
            pending_codes=(),
            pending_messages=(),
            observation=None,
            serialization_unit="BRL",
        ),
    )
    pendings = (
        PendingItem(
            pending_id="pend-00001",
            severity=PendingSeverity.MEDIUM,
            company_code="72",
            competence="03/2024",
            employee_key="col-001",
            employee_name="Ana Lima",
            domain_registration="123",
            event_name="vale_transporte",
            source=SourceRef("LANCAMENTOS_FACEIS", 2, "N2", "vale_transporte"),
            pending_code=PendingCode.NON_AUTOMATABLE_EVENT,
            description="O evento 'vale_transporte' nao e automatizado nesta etapa e nao gerou movimento canonico.",
            recommended_action="Avaliar manualmente este evento e decidir o tratamento antes de exportar.",
        ),
    )
    return IngestionResult(
        parameters=parameters,
        employees=employees,
        movements=movements,
        pendings=pendings,
    )


def test_snapshot_serialization_is_stable():
    result = _sample_result()

    payload_a = serialize_ingestion_result(result, engine_version="0.1.0", status="success_with_pending")
    payload_b = serialize_ingestion_result(result, engine_version="0.1.0", status="success_with_pending")
    json_a = render_ingestion_snapshot_json(result, engine_version="0.1.0", status="success_with_pending")
    json_b = render_ingestion_snapshot_json(result, engine_version="0.1.0", status="success_with_pending")

    assert payload_a == payload_b
    assert json_a == json_b

    parsed = json.loads(json_a)
    assert parsed["execution"]["engine_version"] == "0.1.0"
    assert parsed["movements"][0]["amount"] == "100.25"
    assert parsed["pendings"][0]["pending_code"] == PendingCode.NON_AUTOMATABLE_EVENT


def test_manifest_builds_with_layout_version_and_counts():
    result = _sample_result()
    manifest = build_ingestion_manifest(
        result,
        run_id="run-001",
        engine_version="0.1.0",
        generated_at=datetime(2024, 3, 31, 12, 0, tzinfo=timezone.utc),
        artifact_hashes={"canonical_snapshot": "sha256:abc"},
        status="success_with_pending",
    )

    assert manifest.run_id == "run-001"
    assert manifest.layout_version == "v1"
    assert manifest.config_version == "v1"
    assert manifest.movement_count == 1
    assert manifest.pending_count == 1
    assert manifest.artifact_hashes["canonical_snapshot"] == "sha256:abc"
