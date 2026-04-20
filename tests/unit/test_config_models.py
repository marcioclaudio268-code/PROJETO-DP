import pytest

from config.models import (
    CompanyConfig,
    EmployeeMapping,
    EventMapping,
    PendingPolicy,
    RunManifest,
)


def test_company_config_loads_and_keeps_nested_models():
    config = CompanyConfig(
        company_code="72",
        company_name="Dela More",
        default_process="11",
        competence="2024-03",
        config_version="2024.03.01",
        event_mappings=[
            EventMapping(event_negocio="20", rubrica_saida="201"),
        ],
        employee_mappings=[
            EmployeeMapping(source_employee_key="col-001", domain_registration="123"),
        ],
        pending_policy=PendingPolicy(
            review_required_event_negocios=["48"],
            review_required_fields=["observacoes"],
        ),
        validation_flags={"block_on_unmapped_event": True},
    )

    assert config.company_name == "Dela More"
    assert config.event_mappings[0].rubrica_saida == "201"
    assert config.employee_mappings[0].domain_registration == "123"
    assert config.pending_policy.review_required_event_negocios == ["48"]
    assert config.validation_flags["block_on_unmapped_event"] is True


def test_company_config_rejects_duplicate_event_mappings():
    with pytest.raises(ValueError, match="duplicate event_negocio values"):
        CompanyConfig(
            company_code="72",
            company_name="Dela More",
            default_process="11",
            competence="2024-03",
            config_version="2024.03.01",
            event_mappings=[
                EventMapping(event_negocio="20", rubrica_saida="201"),
                EventMapping(event_negocio="20", rubrica_saida="202"),
            ],
        )


def test_run_manifest_loads():
    manifest = RunManifest(
        run_id="run-001",
        engine_version="0.1.0",
        company_code="72",
        company_name="Dela More",
        competence="2024-03",
        config_version="2024.03.01",
        artifact_hashes={
            "input_xlsx": "sha256:abc",
            "output_txt": "sha256:def",
        },
    )

    assert manifest.pending_count == 0
    assert manifest.artifact_hashes["input_xlsx"] == "sha256:abc"
