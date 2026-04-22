from __future__ import annotations

import json
import shutil
from copy import deepcopy
from pathlib import Path

import pytest

from ingestion.pipeline import ingest_fill_and_persist_planilha_padrao_v1
from mapping.pipeline import map_snapshot_with_company_config
from serialization.pipeline import serialize_mapped_artifact_to_txt
from validation.pipeline import validate_pipeline_v1


REPO_ROOT = Path(__file__).resolve().parents[2]
GOLDEN_ROOT = REPO_ROOT / "data" / "golden" / "v1"


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _normalize_serialization_summary(payload: dict) -> dict:
    normalized = deepcopy(payload)
    normalized["input"]["mapped_artifact_path"] = "<mapped_artifact_path>"
    normalized["txt_path"] = "<txt_path>"
    return normalized


def _normalize_validation_result(payload: dict) -> dict:
    normalized = deepcopy(payload)
    normalized["inputs"]["snapshot_path"] = "<snapshot_path>"
    normalized["inputs"]["mapped_artifact_path"] = "<mapped_artifact_path>"
    normalized["inputs"]["txt_path"] = "<txt_path>"
    normalized["inputs"]["serialization_summary_path"] = "<serialization_summary_path>"
    normalized["inputs"]["serialization_summary_sha256"] = "<volatile:serialization_summary_sha256>"
    return normalized


def _run_case(tmp_path: Path, case_name: str) -> dict[str, Path]:
    case_root = GOLDEN_ROOT / case_name
    work_root = tmp_path / case_name
    work_root.mkdir(parents=True, exist_ok=True)

    input_path = work_root / "input.xlsx"
    config_path = work_root / "company_config.json"
    shutil.copy2(case_root / "input.xlsx", input_path)
    shutil.copy2(case_root / "company_config.json", config_path)

    workbook_output = work_root / "workbook_with_technical_tabs.xlsx"
    snapshot_path = work_root / "snapshot.json"
    mapped_path = work_root / "mapped.json"
    txt_path = work_root / "output.txt"
    serialization_summary_path = work_root / "serialization.json"
    validation_path = work_root / "validation.json"

    ingest_fill_and_persist_planilha_padrao_v1(
        input_path,
        output_path=workbook_output,
        snapshot_path=snapshot_path,
        write_manifest_file=False,
    )
    map_snapshot_with_company_config(snapshot_path, config_path, output_path=mapped_path)
    serialize_mapped_artifact_to_txt(
        mapped_path,
        txt_path=txt_path,
        summary_path=serialization_summary_path,
    )
    validate_pipeline_v1(
        snapshot_path=snapshot_path,
        mapped_artifact_path=mapped_path,
        txt_path=txt_path,
        serialization_summary_path=serialization_summary_path,
        output_path=validation_path,
    )

    return {
        "snapshot": snapshot_path,
        "mapped": mapped_path,
        "txt": txt_path,
        "serialization": serialization_summary_path,
        "validation": validation_path,
    }


@pytest.mark.parametrize(
    ("case_name", "expected_validation_status", "expected_serialized_lines"),
    (
        ("happy_path", "success", 2),
        ("warning_exclusion", "success_with_warnings", 2),
    ),
)
def test_pipeline_v1_matches_golden_case(
    tmp_path: Path,
    case_name: str,
    expected_validation_status: str,
    expected_serialized_lines: int,
) -> None:
    actual_paths = _run_case(tmp_path, case_name)
    expected_root = GOLDEN_ROOT / case_name

    actual_snapshot = _load_json(actual_paths["snapshot"])
    expected_snapshot = _load_json(expected_root / "expected.snapshot.json")
    assert actual_snapshot == expected_snapshot

    actual_mapped = _load_json(actual_paths["mapped"])
    expected_mapped = _load_json(expected_root / "expected.mapped.json")
    assert actual_mapped == expected_mapped

    actual_txt = actual_paths["txt"].read_text(encoding="utf-8")
    expected_txt = (expected_root / "expected.txt").read_text(encoding="utf-8")
    assert actual_txt == expected_txt

    actual_serialization = _normalize_serialization_summary(_load_json(actual_paths["serialization"]))
    expected_serialization = _load_json(expected_root / "expected.serialization.json")
    assert actual_serialization == expected_serialization

    actual_validation = _normalize_validation_result(_load_json(actual_paths["validation"]))
    expected_validation = _load_json(expected_root / "expected.validation.json")
    assert actual_validation == expected_validation

    assert actual_snapshot["counts"]["movements"] == actual_mapped["counts"]["mapped_movements"]
    assert actual_serialization["counts"]["serialized"] == expected_serialized_lines
    assert actual_serialization["counts"]["serialized"] == len(actual_txt.splitlines())
    assert actual_validation["validation_summary"]["actual_txt_lines"] == len(actual_txt.splitlines())
    assert actual_validation["execution"]["status"] == expected_validation_status
    if expected_validation_status == "success_with_warnings":
        assert actual_serialization["counts"]["non_serialized"] > 0
