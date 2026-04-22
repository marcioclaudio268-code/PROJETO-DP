from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import pytest

from config import CompanyConfig
from domain import (
    CanonicalMovement,
    IngestionResult,
    PayrollFileParameters,
    SourceRef,
    ValueType,
)
from ingestion import serialize_ingestion_result
from mapping import (
    EmployeeResolutionSource,
    MappingConfigurationError,
    MappingPendingCode,
    MappingStatus,
    build_snapshot_summary,
    load_company_config,
    map_ingestion_result,
    render_mapping_result_json,
)


def _sample_ingestion_result(
    *,
    employee_key: str | None = "col-001",
    domain_registration: str | None = "123",
    blocked: bool = False,
    pending_codes: tuple[str, ...] = (),
    event_name: str = "gratificacao",
) -> IngestionResult:
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
            employee_key=employee_key,
            employee_name="Ana Lima",
            domain_registration=domain_registration,
            event_name=event_name,
            value_type=ValueType.MONETARY,
            quantity=None,
            hours=None,
            amount=Decimal("100.00"),
            source=SourceRef("LANCAMENTOS_FACEIS", 2, "H2", "gratificacao"),
            blocked=blocked,
            pending_codes=pending_codes,
            pending_messages=tuple(f"pending={item}" for item in pending_codes),
            observation=None,
            serialization_unit="BRL",
        ),
    )
    return IngestionResult(
        parameters=parameters,
        employees=(),
        movements=movements,
        pendings=(),
    )


def _snapshot_summary(result: IngestionResult):
    return build_snapshot_summary(serialize_ingestion_result(result, engine_version="0.1.0"))


def _company_config(**overrides) -> CompanyConfig:
    payload = {
        "company_code": "72",
        "company_name": "Dela More",
        "default_process": "11",
        "competence": "03/2024",
        "config_version": "2024.03.01",
        "event_mappings": [
            {"event_negocio": "gratificacao", "rubrica_saida": "201", "active": True},
        ],
        "employee_mappings": [
            {"source_employee_key": "col-001", "domain_registration": "123", "active": True},
        ],
        "pending_policy": {
            "review_required_event_negocios": [],
            "review_required_fields": [],
            "block_on_ambiguous_observations": True,
            "block_on_unmapped_employee": True,
            "block_on_unmapped_event": True,
        },
    }
    payload.update(overrides)
    return CompanyConfig.model_validate(payload)


def test_load_company_config_reads_json(tmp_path: Path):
    config_path = tmp_path / "company_config.json"
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

    config = load_company_config(config_path)

    assert config.company_code == "72"
    assert config.event_mappings[0].rubrica_saida == "201"


def test_load_company_config_rejects_invalid_duplicate_event_mapping(tmp_path: Path):
    config_path = tmp_path / "company_config_invalid.json"
    config_path.write_text(
        json.dumps(
            {
                "company_code": "72",
                "company_name": "Dela More",
                "default_process": "11",
                "competence": "03/2024",
                "config_version": "2024.03.01",
                "event_mappings": [
                    {"event_negocio": "gratificacao", "rubrica_saida": "201"},
                    {"event_negocio": "gratificacao", "rubrica_saida": "202"},
                ],
                "employee_mappings": [],
                "pending_policy": {"block_on_unmapped_employee": True, "block_on_unmapped_event": True},
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(MappingConfigurationError):
        load_company_config(config_path)


def test_mapping_resolves_registration_from_config_when_snapshot_has_none():
    result = _sample_ingestion_result(domain_registration=None, blocked=False)

    mapped = map_ingestion_result(
        result,
        _company_config(),
        snapshot_summary=_snapshot_summary(result),
    )

    movement = mapped.mapped_movements[0]
    assert movement.resolved_domain_registration == "123"
    assert movement.employee_resolution_source == EmployeeResolutionSource.CONFIG_EMPLOYEE_KEY
    assert movement.output_rubric == "201"
    assert movement.status == MappingStatus.READY


def test_mapping_creates_pending_for_registration_conflict():
    result = _sample_ingestion_result(domain_registration="999", blocked=False)

    mapped = map_ingestion_result(
        result,
        _company_config(),
        snapshot_summary=_snapshot_summary(result),
    )

    assert mapped.mapped_movements[0].status == MappingStatus.BLOCKED
    assert mapped.mapped_movements[0].resolved_domain_registration is None
    assert any(
        pending.pending_code == MappingPendingCode.EMPLOYEE_MAPPING_CONFLICT for pending in mapped.pendings
    )


def test_mapping_creates_pending_when_registration_cannot_be_resolved():
    result = _sample_ingestion_result(employee_key=None, domain_registration=None, blocked=False)

    mapped = map_ingestion_result(
        result,
        _company_config(employee_mappings=[]),
        snapshot_summary=_snapshot_summary(result),
    )

    assert mapped.mapped_movements[0].status == MappingStatus.BLOCKED
    assert any(
        pending.pending_code == MappingPendingCode.EMPLOYEE_MAPPING_MISSING for pending in mapped.pendings
    )


def test_mapping_creates_pending_when_employee_mapping_is_ambiguous():
    result = _sample_ingestion_result(employee_key="shared-key", domain_registration=None, blocked=False)
    config = _company_config(
        employee_mappings=[
            {
                "source_employee_key": "col-001",
                "domain_registration": "123",
                "aliases": ["shared-key"],
            },
            {
                "source_employee_key": "col-002",
                "domain_registration": "456",
                "aliases": ["shared-key"],
            },
        ]
    )

    mapped = map_ingestion_result(
        result,
        config,
        snapshot_summary=_snapshot_summary(result),
    )

    assert mapped.mapped_movements[0].status == MappingStatus.BLOCKED
    assert any(
        pending.pending_code == MappingPendingCode.EMPLOYEE_MAPPING_AMBIGUOUS for pending in mapped.pendings
    )


def test_mapping_creates_pending_for_missing_event_mapping():
    result = _sample_ingestion_result(event_name="bonus", blocked=False)

    mapped = map_ingestion_result(
        result,
        _company_config(),
        snapshot_summary=_snapshot_summary(result),
    )

    assert mapped.mapped_movements[0].output_rubric is None
    assert any(
        pending.pending_code == MappingPendingCode.EVENT_MAPPING_MISSING for pending in mapped.pendings
    )


def test_mapping_creates_pending_for_inactive_event_mapping():
    result = _sample_ingestion_result(blocked=False)
    config = _company_config(
        event_mappings=[
            {"event_negocio": "gratificacao", "rubrica_saida": "201", "active": False},
        ]
    )

    mapped = map_ingestion_result(
        result,
        config,
        snapshot_summary=_snapshot_summary(result),
    )

    assert mapped.mapped_movements[0].output_rubric is None
    assert any(
        pending.pending_code == MappingPendingCode.EVENT_MAPPING_INACTIVE for pending in mapped.pendings
    )


def test_mapping_result_json_is_stable():
    result = _sample_ingestion_result(domain_registration=None, blocked=False)
    mapped = map_ingestion_result(
        result,
        _company_config(),
        snapshot_summary=_snapshot_summary(result),
    )

    json_a = render_mapping_result_json(mapped, engine_version="0.1.0", status="success")
    json_b = render_mapping_result_json(mapped, engine_version="0.1.0", status="success")
    payload = json.loads(json_a)

    assert json_a == json_b
    assert payload["config"]["config_version"] == "2024.03.01"
    assert payload["mapped_movements"][0]["output_rubric"] == "201"

