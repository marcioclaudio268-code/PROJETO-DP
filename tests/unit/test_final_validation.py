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
from serialization import (
    deserialize_mapped_artifact,
    encode_mapped_movement_to_txt_line,
    render_serialization_summary_json,
    render_serialized_txt,
    serialize_loaded_mapped_artifact,
)
from validation import (
    ValidationStatus,
    load_serialization_summary,
    load_txt_lines,
    split_layout_43_line,
    validate_layout_43_structural_fields,
    validate_pipeline_v1,
)


def _movement_definition(
    *,
    movement_id: str,
    employee_key: str,
    registration: str | None,
    rubric: str | None,
    status: str,
    amount: str | None = "100",
    event_name: str = "gratificacao",
) -> dict:
    return {
        "canonical_movement_id": movement_id,
        "company_code": "72",
        "competence": "03/2024",
        "payroll_type": "mensal",
        "default_process": "11",
        "employee_key": employee_key,
        "employee_name": "Ana Lima",
        "event_name": event_name,
        "value_type": "monetario",
        "quantity": None,
        "hours": None,
        "amount": amount,
        "source": {
            "sheet_name": "LANCAMENTOS_FACEIS",
            "row_number": 2,
            "cell": "H2",
            "column_name": "gratificacao",
        },
        "canonical_domain_registration": registration,
        "resolved_domain_registration": registration,
        "output_rubric": rubric,
        "status": status,
        "canonical_blocked": status == "bloqueado",
        "inherited_pending_codes": [],
        "inherited_pending_messages": [],
        "mapping_pending_codes": [],
        "mapping_pending_messages": [],
        "observation": None,
        "informed_rubric": None,
        "event_nature": None,
        "serialization_unit": "BRL",
    }


def _mapped_artifact_payload(movements: list[dict]) -> dict:
    ready_count = sum(1 for item in movements if item["status"] == "pronto_para_serializer")
    blocked_count = sum(1 for item in movements if item["status"] == "bloqueado")
    return {
        "artifact_version": "mapping_result_v1",
        "execution": {"engine_version": "0.1.0", "status": "success"},
        "snapshot": {
            "snapshot_version": "ingestion_snapshot_v1",
            "company_code": "72",
            "company_name": "Dela More",
            "competence": "03/2024",
            "layout_version": "v1",
            "movement_count": len(movements),
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
        "mapped_movements": movements,
        "mapping_pendings": [],
        "counts": {
            "mapped_movements": len(movements),
            "ready_movements": ready_count,
            "blocked_movements": blocked_count,
            "mapping_pendings": 0,
            "blocking_mapping_pendings": 0,
        },
    }


def _snapshot_result(movements: list[dict]) -> IngestionResult:
    parameters = PayrollFileParameters(
        company_code="72",
        company_name="Dela More",
        competence="03/2024",
        payroll_type="mensal",
        default_process="11",
        layout_version="v1",
        source_cells={"empresa_codigo": "B2"},
    )
    canonical_movements = tuple(
        CanonicalMovement(
            movement_id=item["canonical_movement_id"],
            company_code="72",
            competence="03/2024",
            payroll_type="mensal",
            default_process="11",
            employee_key=item["employee_key"],
            employee_name=item["employee_name"],
            domain_registration=item["canonical_domain_registration"],
            event_name=item["event_name"],
            value_type=ValueType.MONETARY,
            quantity=None,
            hours=None,
            amount=Decimal(item["amount"] or "0"),
            source=SourceRef("LANCAMENTOS_FACEIS", 2, "H2", "gratificacao"),
            blocked=item["canonical_blocked"],
            serialization_unit="BRL",
        )
        for item in movements
    )
    return IngestionResult(
        parameters=parameters,
        employees=(),
        movements=canonical_movements,
        pendings=(),
    )


def _write_artifacts(
    tmp_path: Path,
    *,
    movements: list[dict],
    txt_override: str | None = None,
    summary_mutator=None,
) -> tuple[Path, Path, Path, Path]:
    snapshot_path = tmp_path / "input.snapshot.json"
    mapped_path = tmp_path / "input.mapped.json"
    txt_path = tmp_path / "input.txt"
    summary_path = tmp_path / "input.serialization.json"

    write_ingestion_snapshot(_snapshot_result(movements), snapshot_path, engine_version="0.1.0")
    mapped_path.write_text(json.dumps(_mapped_artifact_payload(movements)), encoding="utf-8")

    artifact = deserialize_mapped_artifact(_mapped_artifact_payload(movements))
    serialization_result = serialize_loaded_mapped_artifact(artifact)

    if txt_override is None:
        txt_path.write_text(render_serialized_txt(serialization_result), encoding="utf-8")
    else:
        txt_path.write_text(txt_override, encoding="utf-8")

    summary_payload = json.loads(
        render_serialization_summary_json(
            serialization_result,
            mapped_artifact_path=mapped_path,
            txt_path=txt_path,
            engine_version="0.1.0",
        )
    )
    if summary_mutator is not None:
        summary_mutator(summary_payload)
    summary_path.write_text(json.dumps(summary_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    return snapshot_path, mapped_path, txt_path, summary_path


def test_load_txt_lines_and_summary(tmp_path: Path):
    movements = [
        _movement_definition(
            movement_id="mov-00001",
            employee_key="col-001",
            registration="123",
            rubric="201",
            status="pronto_para_serializer",
        )
    ]
    _, mapped_path, txt_path, summary_path = _write_artifacts(tmp_path, movements=movements)

    lines = load_txt_lines(txt_path)
    summary = load_serialization_summary(summary_path)

    assert lines == ("1000000001230002010072110000000000000010000",)
    assert summary.total_mapped_movements == 1
    assert summary.serialized == 1
    assert summary.mapped_artifact_path == str(mapped_path)


def test_validate_layout_43_structural_fields_accepts_semantic_line():
    fields = validate_layout_43_structural_fields("1000000001230002010072110000000000000010000")

    assert fields["tipo_registro"] == "1"
    assert fields["matricula_dominio"] == "00000000123"
    assert split_layout_43_line("1000000001230002010072110000000000000010000")["valor"] == "0000010000"


def test_final_validation_reports_clean_success(tmp_path: Path):
    movements = [
        _movement_definition(
            movement_id="mov-00001",
            employee_key="col-001",
            registration="123",
            rubric="201",
            status="pronto_para_serializer",
        )
    ]
    snapshot_path, mapped_path, txt_path, summary_path = _write_artifacts(tmp_path, movements=movements)

    artifacts = validate_pipeline_v1(
        snapshot_path=snapshot_path,
        mapped_artifact_path=mapped_path,
        txt_path=txt_path,
        serialization_summary_path=summary_path,
    )

    assert artifacts.result.status == ValidationStatus.SUCCESS
    assert len(artifacts.result.fatal_errors) == 0
    assert len(artifacts.result.inconsistencies) == 0
    assert len(artifacts.result.warnings) == 0
    assert artifacts.output_path.exists()


def test_final_validation_reports_success_with_warnings_when_exclusions_are_consistent(tmp_path: Path):
    movements = [
        _movement_definition(
            movement_id="mov-00001",
            employee_key="col-001",
            registration="123",
            rubric="201",
            status="pronto_para_serializer",
        ),
        _movement_definition(
            movement_id="mov-00002",
            employee_key="col-002",
            registration="456",
            rubric="202",
            status="bloqueado",
        ),
    ]
    snapshot_path, mapped_path, txt_path, summary_path = _write_artifacts(tmp_path, movements=movements)

    artifacts = validate_pipeline_v1(
        snapshot_path=snapshot_path,
        mapped_artifact_path=mapped_path,
        txt_path=txt_path,
        serialization_summary_path=summary_path,
    )

    assert artifacts.result.status == ValidationStatus.SUCCESS_WITH_WARNINGS
    assert len(artifacts.result.warnings) == 1
    assert artifacts.result.warnings[0].code == "movimentos_excluidos_com_consistencia"


def test_final_validation_blocks_when_summary_diverges_from_txt(tmp_path: Path):
    movements = [
        _movement_definition(
            movement_id="mov-00001",
            employee_key="col-001",
            registration="123",
            rubric="201",
            status="pronto_para_serializer",
        )
    ]
    snapshot_path, mapped_path, txt_path, summary_path = _write_artifacts(
        tmp_path,
        movements=movements,
        txt_override="",
    )

    artifacts = validate_pipeline_v1(
        snapshot_path=snapshot_path,
        mapped_artifact_path=mapped_path,
        txt_path=txt_path,
        serialization_summary_path=summary_path,
    )

    assert artifacts.result.status == ValidationStatus.BLOCKED
    issue_codes = {issue.code for issue in artifacts.result.fatal_errors} | {
        issue.code for issue in artifacts.result.inconsistencies
    }
    assert "contagem_txt_divergente" in issue_codes
    assert "movimento_pronto_ausente_no_txt" in issue_codes


def test_final_validation_blocks_when_blocked_item_appears_in_txt(tmp_path: Path):
    movements = [
        _movement_definition(
            movement_id="mov-00001",
            employee_key="col-001",
            registration="123",
            rubric="201",
            status="bloqueado",
        )
    ]
    blocked_line = encode_mapped_movement_to_txt_line(
        deserialize_mapped_artifact(_mapped_artifact_payload(movements)).movements[0]
    )
    snapshot_path, mapped_path, txt_path, summary_path = _write_artifacts(
        tmp_path,
        movements=movements,
        txt_override=blocked_line + "\n",
    )

    artifacts = validate_pipeline_v1(
        snapshot_path=snapshot_path,
        mapped_artifact_path=mapped_path,
        txt_path=txt_path,
        serialization_summary_path=summary_path,
    )

    assert artifacts.result.status == ValidationStatus.BLOCKED
    issue_codes = {issue.code for issue in artifacts.result.inconsistencies}
    assert "movimento_bloqueado_serializado" in issue_codes


def test_final_validation_persists_json_artifact(tmp_path: Path):
    movements = [
        _movement_definition(
            movement_id="mov-00001",
            employee_key="col-001",
            registration="123",
            rubric="201",
            status="pronto_para_serializer",
        )
    ]
    snapshot_path, mapped_path, txt_path, summary_path = _write_artifacts(tmp_path, movements=movements)

    artifacts = validate_pipeline_v1(
        snapshot_path=snapshot_path,
        mapped_artifact_path=mapped_path,
        txt_path=txt_path,
        serialization_summary_path=summary_path,
    )
    payload = json.loads(artifacts.output_path.read_text(encoding="utf-8"))

    assert payload["artifact_version"] == "final_validation_v1"
    assert payload["execution"]["status"] == "success"
    assert payload["validation_summary"]["fatal_error_count"] == 0
    assert payload["inputs"]["txt_path"].endswith(".txt")
