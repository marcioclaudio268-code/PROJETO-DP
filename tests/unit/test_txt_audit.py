from __future__ import annotations

from pathlib import Path

from dashboard.models import (
    DashboardConfigResolution,
    DashboardPaths,
    DashboardProfileResolution,
    DashboardRunResult,
    DashboardState,
    DashboardSummary,
)
from dashboard.txt_audit import build_txt_audit


def _txt_line(
    *,
    registration: str = "123",
    rubric: str = "201",
    company: str = "72",
    process: str = "11",
    reference: str = "000000000",
    value: str = "0000015000",
) -> str:
    return (
        "1"
        + registration.zfill(11)
        + rubric.zfill(6)
        + company.zfill(4)
        + process.zfill(2)
        + reference
        + value
    )


def _movement(
    movement_id: str = "mov-001",
    *,
    registration: str = "123",
    rubric: str = "201",
    employee_name: str | None = "Ana Lima",
    value_type: str = "monetario",
    amount: str = "150",
    hours_text: str | None = None,
    hours_total_minutes: int | None = None,
    quantity: str | None = None,
) -> dict:
    return {
        "canonical_movement_id": movement_id,
        "company_code": "72",
        "competence": "03/2024",
        "payroll_type": "mensal",
        "default_process": "11",
        "employee_key": "col-001",
        "employee_name": employee_name,
        "event_name": "gratificacao",
        "value_type": value_type,
        "quantity": quantity,
        "hours": (
            {"text": hours_text, "total_minutes": hours_total_minutes}
            if hours_text is not None and hours_total_minutes is not None
            else None
        ),
        "amount": amount if value_type == "monetario" else None,
        "source": {
            "sheet_name": "LANCAMENTOS_FACEIS",
            "row_number": 2,
            "cell": "H2",
            "column_name": "gratificacao",
        },
        "canonical_domain_registration": registration,
        "resolved_domain_registration": registration,
        "employee_resolution_source": "snapshot_canonico",
        "output_rubric": rubric,
        "rubric_resolution_source": "config_event_mapping",
        "status": "pronto_para_serializer",
        "canonical_blocked": False,
        "inherited_pending_codes": [],
        "inherited_pending_messages": [],
        "mapping_pending_codes": [],
        "mapping_pending_messages": [],
        "observation": None,
        "informed_rubric": None,
        "event_nature": None,
        "serialization_unit": None,
    }


def _result(tmp_path: Path, *, lines: list[str], movements: list[dict]) -> DashboardRunResult:
    txt_path = tmp_path / "input.txt"
    txt_path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
    paths = DashboardPaths(
        run_root=tmp_path,
        inputs_dir=tmp_path / "inputs",
        artifacts_dir=tmp_path,
        state_path=tmp_path / "dashboard_state.json",
        raw_workbook_path=tmp_path / "input.raw.xlsx",
        editable_workbook_path=tmp_path / "input.xlsx",
        editable_config_path=tmp_path / "company_config.json",
        analyzed_workbook_path=tmp_path / "analyzed.xlsx",
        snapshot_path=tmp_path / "input.snapshot.json",
        manifest_path=tmp_path / "input.manifest.json",
        normalization_path=tmp_path / "input.normalization.json",
        mapped_artifact_path=tmp_path / "input.mapped.json",
        txt_path=txt_path,
        serialization_summary_path=tmp_path / "input.serialization.json",
        validation_path=tmp_path / "input.validation.json",
    )
    summary = DashboardSummary(
        company_name="Dela More",
        company_code="72",
        competence="03/2024",
        employee_count=1,
        relevant_movement_count=len(movements),
        pending_count=0,
        ignored_count=0,
        serialized_line_count=len(lines),
        validation_status="success",
        status_label="OK",
        recommendation="OK",
        txt_enabled=True,
        txt_status_label="Liberado",
        config_status="FOUND",
        config_status_label="Encontrada",
        config_source="test",
        config_version="test",
    )
    return DashboardRunResult(
        paths=paths,
        state=DashboardState(session_version="test", source_workbook_name="input.xlsx"),
        summary=summary,
        config_resolution=DashboardConfigResolution(
            status="FOUND",
            status_label="Encontrada",
            message="OK",
            company_code="72",
            competence="03/2024",
            config_source="test",
            config_version="test",
            source_path=None,
        ),
        profile_resolution=DashboardProfileResolution(
            status="not_required",
            status_label="Nao requerido",
            message="OK",
            company_code="72",
            competence="03/2024",
            layout_id="template_v1_canonico",
            source_path=None,
        ),
        pendings=(),
        snapshot_payload={},
        mapped_payload={"mapped_movements": movements},
        serialization_payload={"counts": {"serialized": len(lines)}},
        validation_payload={"execution": {"status": "success"}},
    )


def test_txt_audit_summarizes_ok_txt_lines(tmp_path: Path) -> None:
    result = _result(tmp_path, lines=[_txt_line()], movements=[_movement()])

    audit = build_txt_audit(result)

    assert audit.summary.total_lines == 1
    assert audit.summary.company_code == "72"
    assert audit.summary.process_codes == ("11",)
    assert audit.summary.rubric_totals[0].rubric == "201"
    assert audit.summary.rubric_totals[0].line_count == 1
    assert audit.summary.rubric_totals[0].value_type == "monetario"
    assert audit.summary.rubric_totals[0].total_value == "150"
    assert audit.summary.rubric_totals[0].display_total == "150"
    assert audit.employee_rows[0].check_status == "OK"
    assert audit.employee_rows[0].employee_name == "Ana Lima"
    assert audit.divergences == ()


def test_txt_audit_totals_hours_by_reference(tmp_path: Path) -> None:
    result = _result(
        tmp_path,
        lines=[
            _txt_line(rubric="200", reference="000000800", value="0000000000"),
            _txt_line(registration="124", rubric="200", reference="000001630", value="0000000000"),
        ],
        movements=[
            _movement("mov-001", rubric="200", value_type="horas", hours_text="08:00", hours_total_minutes=480),
            _movement(
                "mov-002",
                registration="124",
                rubric="200",
                value_type="horas",
                hours_text="16:30",
                hours_total_minutes=990,
            ),
        ],
    )

    audit = build_txt_audit(result)

    total = audit.summary.rubric_totals[0]
    assert total.rubric == "200"
    assert total.value_type == "horas"
    assert total.total_reference == "24:30"
    assert total.display_total == "24:30"
    assert {row.check_status for row in audit.employee_rows} == {"OK"}


def test_txt_audit_totals_days_by_reference(tmp_path: Path) -> None:
    result = _result(
        tmp_path,
        lines=[
            _txt_line(rubric="8792", reference="000000100", value="0000000000"),
            _txt_line(registration="124", rubric="8792", reference="000000200", value="0000000000"),
        ],
        movements=[
            _movement("mov-001", rubric="8792", value_type="dias", quantity="1"),
            _movement("mov-002", registration="124", rubric="8792", value_type="dias", quantity="2"),
        ],
    )

    audit = build_txt_audit(result)

    total = audit.summary.rubric_totals[0]
    assert total.rubric == "8792"
    assert total.value_type == "dias"
    assert total.total_reference == "3"
    assert total.display_total == "3 dia(s)"
    assert {row.check_status for row in audit.employee_rows} == {"OK"}


def test_txt_audit_flags_value_and_rubric_divergences(tmp_path: Path) -> None:
    result = _result(
        tmp_path,
        lines=[
            _txt_line(value="0000014000"),
            _txt_line(rubric="202"),
        ],
        movements=[_movement()],
    )

    audit = build_txt_audit(result)

    assert [row.check_status for row in audit.employee_rows] == [
        "VALOR_DIVERGENTE",
        "RUBRICA_DIVERGENTE",
    ]
    assert {item.code for item in audit.divergences} == {
        "VALOR_DIVERGENTE",
        "RUBRICA_DIVERGENTE",
    }


def test_txt_audit_flags_duplicate_and_missing_employee_name(tmp_path: Path) -> None:
    result = _result(
        tmp_path,
        lines=[_txt_line(), _txt_line()],
        movements=[_movement("mov-001", employee_name=None), _movement("mov-002")],
    )

    audit = build_txt_audit(result)

    assert any(row.check_status == "DUPLICADO" for row in audit.employee_rows)
    assert {item.code for item in audit.divergences} >= {
        "DUPLICADO",
        "MATRICULA_SEM_NOME",
    }


def test_txt_audit_flags_line_not_found_in_sheet(tmp_path: Path) -> None:
    result = _result(tmp_path, lines=[_txt_line()], movements=[])

    audit = build_txt_audit(result)

    assert audit.employee_rows[0].check_status == "NAO_LOCALIZADO_NA_FOLHA"
    assert audit.divergences[0].code == "NAO_LOCALIZADO_NA_FOLHA"


def test_txt_audit_flags_invalid_txt_line_without_raising(tmp_path: Path) -> None:
    result = _result(tmp_path, lines=["1" + "123".zfill(11)], movements=[])

    audit = build_txt_audit(result)

    assert audit.employee_rows[0].check_status == "LINHA_TXT_INVALIDA"
    assert audit.divergences[0].code == "LINHA_TXT_INVALIDA"


def test_txt_audit_handles_empty_txt(tmp_path: Path) -> None:
    result = _result(tmp_path, lines=[], movements=[])

    audit = build_txt_audit(result)

    assert audit.summary.total_lines == 0
    assert audit.summary.rubric_totals == ()
    assert audit.employee_rows == ()
    assert audit.divergences == ()
