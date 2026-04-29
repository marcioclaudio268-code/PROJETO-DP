from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from dashboard import (
    CompanyEmployeeRecord,
    CompanyEmployeeRegistry,
    apply_employee_registry_to_config_payload,
    find_employee_by_domain_registration,
    find_employee_by_key,
    find_employee_by_name_or_alias,
    list_active_employees,
    load_company_employee_registry,
    save_company_employee_registry,
    upsert_employee_record,
)


def _employee(
    *,
    employee_key: str | None = "col-001",
    employee_name: str = "ANA CLEIA FILETTO BARBOSA",
    domain_registration: str = "50006",
    aliases: list[str] | None = None,
    status: str = "active",
) -> CompanyEmployeeRecord:
    return CompanyEmployeeRecord(
        employee_key=employee_key,
        employee_name=employee_name,
        domain_registration=domain_registration,
        aliases=aliases or [],
        status=status,
        source="test",
    )


def test_company_employee_registry_saves_and_loads_valid_registry(tmp_path: Path) -> None:
    registry = CompanyEmployeeRegistry(
        company_code="72",
        company_name="Dela More",
        employees=[_employee(aliases=["ANA CLEIA"])],
    )

    path = save_company_employee_registry(registry, root=tmp_path)
    loaded = load_company_employee_registry("72", root=tmp_path)

    assert path == tmp_path / "72.json"
    assert loaded.company_code == "72"
    assert loaded.company_name == "Dela More"
    assert loaded.employees[0].domain_registration == "50006"
    assert json.loads(path.read_text(encoding="utf-8"))["employees"][0]["aliases"] == ["ANA CLEIA"]


def test_company_employee_registry_rejects_empty_domain_registration() -> None:
    with pytest.raises(ValidationError):
        _employee(domain_registration="")


def test_company_employee_registry_rejects_duplicate_active_domain_registration() -> None:
    with pytest.raises(ValidationError):
        CompanyEmployeeRegistry(
            company_code="72",
            employees=[
                _employee(employee_key="col-001", domain_registration="50006"),
                _employee(employee_key="col-002", domain_registration="50006"),
            ],
        )


def test_company_employee_registry_finds_by_name_alias_key_and_registration() -> None:
    registry = CompanyEmployeeRegistry(
        company_code="72",
        employees=[_employee(aliases=["ANA CLEIA", "A C FILETTO"])],
    )

    assert find_employee_by_key(registry, "col-001")[0].domain_registration == "50006"
    assert find_employee_by_name_or_alias(registry, "ana cleia")[0].domain_registration == "50006"
    assert find_employee_by_name_or_alias(registry, "ANA CLEIA FILETTO BARBOSA")[0].domain_registration == "50006"
    assert find_employee_by_domain_registration(registry, "50006")[0].employee_name == "ANA CLEIA FILETTO BARBOSA"


def test_company_employee_registry_upserts_employee_by_key() -> None:
    registry = CompanyEmployeeRegistry(company_code="72", employees=[_employee()])
    updated = upsert_employee_record(
        registry,
        _employee(employee_key="col-001", employee_name="ANA CLEIA", domain_registration="60006"),
    )

    assert len(updated.employees) == 1
    assert updated.employees[0].employee_name == "ANA CLEIA"
    assert updated.employees[0].domain_registration == "60006"


def test_company_employee_registry_lists_only_active_employees() -> None:
    registry = CompanyEmployeeRegistry(
        company_code="72",
        employees=[
            _employee(employee_key="col-001", domain_registration="50006", status="active"),
            _employee(employee_key="col-002", domain_registration="50007", status="inactive"),
        ],
    )

    assert [employee.employee_key for employee in list_active_employees(registry)] == ["col-001"]


def test_employee_registry_applies_mapping_to_config_on_safe_match() -> None:
    config_payload = {"employee_mappings": []}
    registry = CompanyEmployeeRegistry(company_code="72", employees=[_employee()])

    result = apply_employee_registry_to_config_payload(
        config_payload,
        registry=registry,
        employee_sources=[
            {
                "employee_key": "col-001",
                "employee_name": "ANA CLEIA",
                "domain_registration": None,
            }
        ],
    )

    assert result.mappings_added == 1
    assert config_payload["employee_mappings"] == [
        {
            "source_employee_key": "col-001",
            "source_employee_name": "ANA CLEIA",
            "domain_registration": "50006",
            "active": True,
            "aliases": [],
            "notes": "Preenchido a partir do cadastro persistente de funcionarios.",
        }
    ]


def test_employee_registry_does_not_apply_mapping_when_match_is_ambiguous() -> None:
    config_payload = {"employee_mappings": []}
    registry = CompanyEmployeeRegistry(
        company_code="72",
        employees=[
            _employee(employee_key="col-001", domain_registration="50006", aliases=["ANA"]),
            _employee(employee_key="col-002", domain_registration="50007", aliases=["ANA"]),
        ],
    )

    result = apply_employee_registry_to_config_payload(
        config_payload,
        registry=registry,
        employee_sources=[
            {
                "employee_key": "source-ana",
                "employee_name": "ANA",
                "domain_registration": None,
            }
        ],
    )

    assert result.mappings_added == 0
    assert result.ambiguous_sources == ("ANA",)
    assert config_payload["employee_mappings"] == []
