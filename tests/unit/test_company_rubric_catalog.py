from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from dashboard import (
    CompanyRubricCatalog,
    CompanyRubricRecord,
    apply_rubric_catalog_to_config_payload,
    find_rubric_by_canonical_event,
    find_rubric_by_code,
    find_rubric_by_description_or_alias,
    list_active_rubrics,
    load_company_rubric_catalog,
    save_company_rubric_catalog,
    upsert_rubric_record,
)


def _rubric(
    *,
    rubric_code: str = "201",
    description: str = "EXTRA 70%",
    canonical_event: str = "horas_extras_70",
    value_kind: str = "horas",
    nature: str = "provento",
    aliases: list[str] | None = None,
    status: str = "active",
) -> CompanyRubricRecord:
    return CompanyRubricRecord(
        rubric_code=rubric_code,
        description=description,
        canonical_event=canonical_event,
        value_kind=value_kind,
        nature=nature,
        aliases=aliases or [],
        status=status,
        source="test",
    )


def test_company_rubric_catalog_saves_and_loads_valid_catalog(tmp_path: Path) -> None:
    catalog = CompanyRubricCatalog(
        company_code="887",
        company_name="BERBELLA",
        rubrics=[_rubric(aliases=["EXTRA SETENTA"])],
    )

    path = save_company_rubric_catalog(catalog, root=tmp_path)
    loaded = load_company_rubric_catalog("887", root=tmp_path)

    assert path == tmp_path / "887.json"
    assert loaded.company_code == "887"
    assert loaded.rubrics[0].rubric_code == "201"
    assert json.loads(path.read_text(encoding="utf-8"))["rubrics"][0]["aliases"] == ["EXTRA SETENTA"]


def test_company_rubric_catalog_rejects_missing_required_fields() -> None:
    with pytest.raises(ValidationError):
        _rubric(rubric_code="")
    with pytest.raises(ValidationError):
        _rubric(description="")
    with pytest.raises(ValidationError):
        _rubric(canonical_event="")


def test_company_rubric_catalog_rejects_invalid_value_kind() -> None:
    with pytest.raises(ValidationError):
        _rubric(value_kind="texto")


def test_company_rubric_catalog_rejects_duplicate_active_rubric_code() -> None:
    with pytest.raises(ValidationError):
        CompanyRubricCatalog(
            company_code="887",
            rubrics=[
                _rubric(rubric_code="201", canonical_event="horas_extras_70"),
                _rubric(rubric_code="201", canonical_event="gratificacao"),
            ],
        )


def test_company_rubric_catalog_finds_by_code_description_alias_and_event() -> None:
    catalog = CompanyRubricCatalog(
        company_code="887",
        rubrics=[_rubric(aliases=["EXTRA SETENTA"])],
    )

    assert find_rubric_by_code(catalog, "201")[0].canonical_event == "horas_extras_70"
    assert find_rubric_by_description_or_alias(catalog, "EXTRA 70%")[0].rubric_code == "201"
    assert find_rubric_by_description_or_alias(catalog, "extra setenta")[0].rubric_code == "201"
    assert find_rubric_by_canonical_event(catalog, "horas_extras_70")[0].rubric_code == "201"


def test_company_rubric_catalog_upserts_rubric_by_code() -> None:
    catalog = CompanyRubricCatalog(company_code="887", rubrics=[_rubric()])
    updated = upsert_rubric_record(
        catalog,
        _rubric(rubric_code="201", description="EXTRA 70 ATUAL", canonical_event="horas_extras_70"),
    )

    assert len(updated.rubrics) == 1
    assert updated.rubrics[0].description == "EXTRA 70 ATUAL"


def test_company_rubric_catalog_lists_only_active_rubrics() -> None:
    catalog = CompanyRubricCatalog(
        company_code="887",
        rubrics=[
            _rubric(rubric_code="201", status="active"),
            _rubric(rubric_code="200", canonical_event="horas_extras_100", status="inactive"),
        ],
    )

    assert [rubric.rubric_code for rubric in list_active_rubrics(catalog)] == ["201"]


def test_rubric_catalog_applies_event_mapping_on_safe_match() -> None:
    config_payload = {"event_mappings": []}
    catalog = CompanyRubricCatalog(company_code="887", rubrics=[_rubric()])

    result = apply_rubric_catalog_to_config_payload(
        config_payload,
        catalog=catalog,
        rubric_sources=[
            {
                "canonical_event": "horas_extras_70",
                "rubric_code": None,
                "description": None,
            }
        ],
    )

    assert result.mappings_added == 1
    assert config_payload["event_mappings"] == [
        {
            "event_negocio": "horas_extras_70",
            "rubrica_saida": "201",
            "active": True,
            "notes": "Preenchido a partir do catalogo persistente de rubricas.",
        }
    ]


def test_rubric_catalog_does_not_apply_event_mapping_when_match_is_ambiguous() -> None:
    config_payload = {"event_mappings": []}
    catalog = CompanyRubricCatalog(
        company_code="887",
        rubrics=[
            _rubric(rubric_code="201", canonical_event="horas_extras_70"),
            _rubric(rubric_code="219", canonical_event="horas_extras_70"),
        ],
    )

    result = apply_rubric_catalog_to_config_payload(
        config_payload,
        catalog=catalog,
        rubric_sources=[
            {
                "canonical_event": "horas_extras_70",
                "rubric_code": None,
                "description": None,
            }
        ],
    )

    assert result.mappings_added == 0
    assert result.ambiguous_sources == ("horas_extras_70",)
    assert config_payload["event_mappings"] == []
