from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from dashboard import (
    ColumnGenerationMode,
    ColumnMappingProfileError,
    ColumnMappingRule,
    ColumnValueKind,
    CompanyColumnMappingProfile,
    column_mapping_profile_path,
    load_column_mapping_profile,
    save_column_mapping_profile,
    upsert_column_mapping_rule,
)


def _sample_profile() -> CompanyColumnMappingProfile:
    return CompanyColumnMappingProfile(
        company_code="887",
        company_name="BERBELLA LTDA",
        default_process="11",
        mappings=[
            ColumnMappingRule(
                column_name="GRAT.",
                enabled=True,
                rubrica_target="20",
                value_kind=ColumnValueKind.MONETARY,
                generation_mode=ColumnGenerationMode.SINGLE_LINE,
                ignore_zero=True,
                ignore_text=True,
            ),
            ColumnMappingRule(
                column_name="ATRASO",
                enabled=True,
                rubrica_target="8069",
                value_kind=ColumnValueKind.HOURS,
                generation_mode=ColumnGenerationMode.SINGLE_LINE,
                ignore_zero=True,
                ignore_text=True,
            ),
            ColumnMappingRule(
                column_name="FALTA",
                enabled=True,
                rubricas_target=["8792", "8794"],
                value_kind=ColumnValueKind.QUANTITY,
                generation_mode=ColumnGenerationMode.MULTI_LINE,
                ignore_zero=True,
                ignore_text=True,
            ),
            ColumnMappingRule(
                column_name="ADIANT. QUINZ",
                enabled=False,
                value_kind=ColumnValueKind.MONETARY,
                generation_mode=ColumnGenerationMode.IGNORE,
                ignore_zero=True,
                ignore_text=True,
            ),
        ],
    )


def test_creates_valid_company_column_mapping_profile() -> None:
    profile = _sample_profile()

    assert profile.company_code == "887"
    assert profile.company_name == "BERBELLA LTDA"
    assert len(profile.mappings) == 4


def test_serializes_profile_to_dict_and_json() -> None:
    profile = _sample_profile()

    payload = profile.model_dump(mode="json")
    json_payload = profile.model_dump_json()

    assert payload["company_code"] == "887"
    assert payload["mappings"][0]["column_name"] == "GRAT."
    assert json.loads(json_payload)["mappings"][2]["rubricas_target"] == ["8792", "8794"]


def test_saves_and_loads_profile_from_json(tmp_path: Path) -> None:
    profile = _sample_profile()

    saved_path = save_column_mapping_profile(profile, root=tmp_path)
    loaded = load_column_mapping_profile("887", root=tmp_path)

    assert saved_path == tmp_path / "887.json"
    assert loaded == profile


def test_simple_column_mapping_targets_one_rubric() -> None:
    mapping = _sample_profile().mappings[0]

    assert mapping.column_name == "GRAT."
    assert mapping.target_rubrics == ("20",)
    assert mapping.value_kind == ColumnValueKind.MONETARY
    assert mapping.generation_mode == ColumnGenerationMode.SINGLE_LINE


def test_hour_column_mapping_targets_one_hour_rubric() -> None:
    mapping = _sample_profile().mappings[1]

    assert mapping.column_name == "ATRASO"
    assert mapping.target_rubrics == ("8069",)
    assert mapping.value_kind == ColumnValueKind.HOURS


def test_composite_column_mapping_targets_two_rubrics() -> None:
    mapping = _sample_profile().mappings[2]

    assert mapping.column_name == "FALTA"
    assert mapping.target_rubrics == ("8792", "8794")
    assert mapping.generation_mode == ColumnGenerationMode.MULTI_LINE


def test_ignored_column_mapping_is_explicit() -> None:
    mapping = _sample_profile().mappings[3]

    assert mapping.column_name == "ADIANT. QUINZ"
    assert mapping.enabled is False
    assert mapping.generation_mode == ColumnGenerationMode.IGNORE
    assert mapping.target_rubrics == ()


def test_rejects_invalid_profile_with_duplicate_column_mapping() -> None:
    with pytest.raises(ValidationError):
        CompanyColumnMappingProfile(
            company_code="887",
            mappings=[
                ColumnMappingRule(
                    column_name="GRAT.",
                    enabled=True,
                    rubrica_target="20",
                    value_kind="monetario",
                    generation_mode="single_line",
                    ignore_zero=True,
                    ignore_text=True,
                ),
                ColumnMappingRule(
                    column_name="GRAT.",
                    enabled=True,
                    rubrica_target="21",
                    value_kind="monetario",
                    generation_mode="single_line",
                    ignore_zero=True,
                    ignore_text=True,
                ),
            ],
        )


def test_rejects_mapping_without_required_fields() -> None:
    with pytest.raises(ValidationError):
        ColumnMappingRule.model_validate(
            {
                "column_name": "GRAT.",
                "enabled": True,
                "generation_mode": "single_line",
                "ignore_zero": True,
                "ignore_text": True,
            }
        )


def test_rejects_invalid_generation_mode() -> None:
    with pytest.raises(ValidationError):
        ColumnMappingRule.model_validate(
            {
                "column_name": "GRAT.",
                "enabled": True,
                "rubrica_target": "20",
                "value_kind": "monetario",
                "generation_mode": "automatico",
                "ignore_zero": True,
                "ignore_text": True,
            }
        )


def test_rejects_invalid_value_kind() -> None:
    with pytest.raises(ValidationError):
        ColumnMappingRule.model_validate(
            {
                "column_name": "GRAT.",
                "enabled": True,
                "rubrica_target": "20",
                "value_kind": "valor",
                "generation_mode": "single_line",
                "ignore_zero": True,
                "ignore_text": True,
            }
        )


def test_rejects_missing_profile_file(tmp_path: Path) -> None:
    with pytest.raises(ColumnMappingProfileError) as exc_info:
        load_column_mapping_profile("887", root=tmp_path)

    assert exc_info.value.code == "profile_not_found"


def test_rejects_unsafe_company_code_for_profile_path() -> None:
    with pytest.raises(ColumnMappingProfileError) as exc_info:
        column_mapping_profile_path("../887")

    assert exc_info.value.code == "invalid_company_code"


def test_upsert_column_mapping_rule_updates_existing_column() -> None:
    profile = _sample_profile()
    updated = upsert_column_mapping_rule(
        profile,
        ColumnMappingRule(
            column_name="GRAT.",
            enabled=True,
            rubrica_target="21",
            value_kind="monetario",
            generation_mode="single_line",
            ignore_zero=True,
            ignore_text=True,
        ),
    )

    assert len(updated.mappings) == len(profile.mappings)
    assert updated.mappings[0].rubrica_target == "21"


def test_upsert_column_mapping_rule_adds_new_column() -> None:
    profile = _sample_profile()
    updated = upsert_column_mapping_rule(
        profile,
        ColumnMappingRule(
            column_name="EXTRA 100%",
            enabled=True,
            rubrica_target="200",
            value_kind="horas",
            generation_mode="single_line",
            ignore_zero=True,
            ignore_text=True,
        ),
    )

    assert len(updated.mappings) == len(profile.mappings) + 1
    assert updated.mappings[-1].column_name == "EXTRA 100%"
