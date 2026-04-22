"""Final validation and reconciliation across ingestion, mapping and serialization artifacts."""

from __future__ import annotations

from collections import Counter
from pathlib import Path

from ingestion import compute_file_sha256, get_engine_version, summarize_ingestion_result
from serialization import (
    encode_mapped_movement_to_txt_line,
    evaluate_serialization_eligibility,
    infer_serialization_status,
    serialize_loaded_mapped_artifact,
)

from .layout import validate_layout_43_structural_fields
from .models import (
    LoadedMappedArtifactEnvelope,
    LoadedSerializationSummary,
    ValidationIssue,
    ValidationResult,
    ValidationSeverity,
    ValidationStatus,
)


FINAL_VALIDATION_ARTIFACT_VERSION = "final_validation_v1"


def validate_final_artifacts(
    *,
    snapshot_result,
    mapped_artifact: LoadedMappedArtifactEnvelope,
    serialization_summary: LoadedSerializationSummary,
    txt_lines: tuple[str, ...],
    mapped_artifact_path: str | Path,
    txt_path: str | Path,
) -> ValidationResult:
    fatal_errors: list[ValidationIssue] = []
    inconsistencies: list[ValidationIssue] = []
    warnings: list[ValidationIssue] = []

    ingestion_summary = summarize_ingestion_result(snapshot_result)
    mapping_summary = _summarize_mapped_artifact(mapped_artifact)
    expected_serialization = serialize_loaded_mapped_artifact(mapped_artifact.artifact)
    expected_serialization_status = infer_serialization_status(expected_serialization)
    actual_txt_path = Path(txt_path)
    actual_mapped_path = Path(mapped_artifact_path)
    actual_txt_sha = compute_file_sha256(actual_txt_path)
    actual_mapped_sha = compute_file_sha256(actual_mapped_path)

    _validate_snapshot_vs_mapping(snapshot_result, mapped_artifact, fatal_errors, inconsistencies)
    _validate_mapping_internal_counts(mapped_artifact, mapping_summary, fatal_errors)
    _validate_serialization_summary_against_expected(
        serialization_summary=serialization_summary,
        expected_serialization=expected_serialization,
        expected_status=expected_serialization_status,
        actual_mapped_sha=actual_mapped_sha,
        actual_txt_sha=actual_txt_sha,
        fatal_errors=fatal_errors,
        inconsistencies=inconsistencies,
    )
    _validate_actual_txt_structure(txt_lines, fatal_errors)
    _reconcile_txt_content(
        mapped_artifact=mapped_artifact,
        expected_serialization=expected_serialization,
        txt_lines=txt_lines,
        inconsistencies=inconsistencies,
    )

    if not fatal_errors and not inconsistencies and expected_serialization.skipped_items:
        warnings.append(
            ValidationIssue(
                code="movimentos_excluidos_com_consistencia",
                severity=ValidationSeverity.WARNING,
                message=(
                    f"{len(expected_serialization.skipped_items)} movimento(s) ficaram fora do TXT de forma consistente com o resumo da serializacao."
                ),
            )
        )

    status = _resolve_validation_status(
        total_mapped_movements=len(mapped_artifact.artifact.movements),
        actual_txt_lines=len(txt_lines),
        fatal_errors=fatal_errors,
        inconsistencies=inconsistencies,
        warnings=warnings,
    )
    validation_summary = {
        "fatal_error_count": len(fatal_errors),
        "inconsistency_count": len(inconsistencies),
        "warning_count": len(warnings),
        "actual_txt_lines": len(txt_lines),
        "expected_txt_lines": len(expected_serialization.serialized_lines),
    }

    return ValidationResult(
        artifact_version=FINAL_VALIDATION_ARTIFACT_VERSION,
        engine_version=get_engine_version(),
        status=status,
        recommendation=_build_recommendation(status),
        human_summary=_build_human_summary(status, validation_summary),
        ingestion_summary=ingestion_summary,
        mapping_summary=mapping_summary,
        serialization_summary={
            "total_mapped_movements": serialization_summary.total_mapped_movements,
            "serialized": serialization_summary.serialized,
            "non_serialized": serialization_summary.non_serialized,
            "blocked_or_non_serialized": serialization_summary.blocked_or_non_serialized,
        },
        validation_summary=validation_summary,
        fatal_errors=tuple(fatal_errors),
        inconsistencies=tuple(inconsistencies),
        warnings=tuple(warnings),
    )


def _summarize_mapped_artifact(mapped_artifact: LoadedMappedArtifactEnvelope) -> dict[str, int]:
    movements = mapped_artifact.artifact.movements
    return {
        "mapped_movements": len(movements),
        "ready_movements": sum(1 for movement in movements if movement.status.value == "pronto_para_serializer"),
        "blocked_movements": sum(1 for movement in movements if movement.status.value == "bloqueado"),
        "mapping_pending_count": mapped_artifact.mapping_pending_count,
    }


def _validate_snapshot_vs_mapping(snapshot_result, mapped_artifact, fatal_errors, inconsistencies) -> None:
    snapshot_ids = tuple(movement.movement_id for movement in snapshot_result.movements)
    mapped_ids = tuple(movement.canonical_movement_id for movement in mapped_artifact.artifact.movements)

    if snapshot_result.parameters.company_code != mapped_artifact.artifact.metadata.company_code:
        fatal_errors.append(
            ValidationIssue(
                code="snapshot_mapping_company_code_divergente",
                severity=ValidationSeverity.FATAL,
                message=(
                    "Snapshot canonico e artefato mapeado pertencem a empresas diferentes."
                ),
            )
        )

    if snapshot_result.parameters.competence != mapped_artifact.artifact.metadata.competence:
        fatal_errors.append(
            ValidationIssue(
                code="snapshot_mapping_competencia_divergente",
                severity=ValidationSeverity.FATAL,
                message="Snapshot canonico e artefato mapeado pertencem a competencias diferentes.",
            )
        )

    if len(snapshot_ids) != len(mapped_ids):
        fatal_errors.append(
            ValidationIssue(
                code="contagem_snapshot_mapping_divergente",
                severity=ValidationSeverity.FATAL,
                message=(
                    f"Snapshot possui {len(snapshot_ids)} movimento(s) e o artefato mapeado possui {len(mapped_ids)}."
                ),
            )
        )

    missing_in_mapping = sorted(set(snapshot_ids) - set(mapped_ids))
    for movement_id in missing_in_mapping:
        inconsistencies.append(
            ValidationIssue(
                code="movimento_canonico_ausente_no_mapping",
                severity=ValidationSeverity.FATAL,
                message=f"Movimento canonico '{movement_id}' nao apareceu no artefato mapeado.",
                canonical_movement_id=movement_id,
            )
        )

    unexpected_in_mapping = sorted(set(mapped_ids) - set(snapshot_ids))
    for movement_id in unexpected_in_mapping:
        inconsistencies.append(
            ValidationIssue(
                code="movimento_mapeado_sem_origem_canonica",
                severity=ValidationSeverity.FATAL,
                message=f"Movimento mapeado '{movement_id}' nao existe no snapshot canonico.",
                canonical_movement_id=movement_id,
            )
        )


def _validate_mapping_internal_counts(mapped_artifact, mapping_summary, fatal_errors) -> None:
    raw_counts = mapped_artifact.raw_counts
    expected = {
        "mapped_movements": mapping_summary["mapped_movements"],
        "ready_movements": mapping_summary["ready_movements"],
        "blocked_movements": mapping_summary["blocked_movements"],
    }

    for key, expected_value in expected.items():
        if key in raw_counts and raw_counts[key] != expected_value:
            fatal_errors.append(
                ValidationIssue(
                    code="contagem_interna_mapping_divergente",
                    severity=ValidationSeverity.FATAL,
                    message=(
                        f"Artefato mapeado declara '{key}={raw_counts[key]}' mas o conteudo real reconstituido e {expected_value}."
                    ),
                )
            )


def _validate_serialization_summary_against_expected(
    *,
    serialization_summary: LoadedSerializationSummary,
    expected_serialization,
    expected_status: str,
    actual_mapped_sha: str,
    actual_txt_sha: str,
    fatal_errors,
    inconsistencies,
) -> None:
    if serialization_summary.mapped_artifact_sha256 != actual_mapped_sha:
        fatal_errors.append(
            ValidationIssue(
                code="hash_artefato_mapeado_divergente",
                severity=ValidationSeverity.FATAL,
                message="O hash do artefato mapeado no resumo da serializacao diverge do arquivo informado.",
            )
        )

    if serialization_summary.txt_sha256 != actual_txt_sha:
        fatal_errors.append(
            ValidationIssue(
                code="hash_txt_divergente",
                severity=ValidationSeverity.FATAL,
                message="O hash do TXT no resumo da serializacao diverge do arquivo informado.",
            )
        )

    if serialization_summary.total_mapped_movements != expected_serialization.total_mapped_movements:
        fatal_errors.append(
            ValidationIssue(
                code="resumo_total_mapeados_divergente",
                severity=ValidationSeverity.FATAL,
                message="A contagem total de movimentos mapeados no resumo da serializacao diverge do artefato mapeado.",
            )
        )

    if serialization_summary.serialized != len(expected_serialization.serialized_lines):
        fatal_errors.append(
            ValidationIssue(
                code="resumo_contagem_serializada_divergente",
                severity=ValidationSeverity.FATAL,
                message="A contagem serializada do resumo diverge da serializacao reconstituida.",
            )
        )

    if serialization_summary.non_serialized != len(expected_serialization.skipped_items):
        fatal_errors.append(
            ValidationIssue(
                code="resumo_contagem_nao_serializada_divergente",
                severity=ValidationSeverity.FATAL,
                message="A contagem nao serializada do resumo diverge da serializacao reconstituida.",
            )
        )

    if serialization_summary.blocked_or_non_serialized != len(expected_serialization.skipped_items):
        fatal_errors.append(
            ValidationIssue(
                code="resumo_contagem_bloqueada_divergente",
                severity=ValidationSeverity.FATAL,
                message="A contagem blocked_or_non_serialized do resumo diverge da serializacao reconstituida.",
            )
        )

    if serialization_summary.execution_status != expected_status:
        fatal_errors.append(
            ValidationIssue(
                code="resumo_status_divergente",
                severity=ValidationSeverity.FATAL,
                message=(
                    f"O resumo da serializacao informa status '{serialization_summary.execution_status}', mas o status reconstituido e '{expected_status}'."
                ),
            )
        )

    expected_reason_counts = Counter(item.reason_code for item in expected_serialization.skipped_items)
    if dict(expected_reason_counts) != serialization_summary.non_serialized_reason_counts:
        inconsistencies.append(
            ValidationIssue(
                code="resumo_motivos_nao_serializados_divergente",
                severity=ValidationSeverity.FATAL,
                message="Os motivos agregados de nao serializacao divergem entre o resumo e o artefato mapeado.",
            )
        )

    expected_skips = {
        (item.canonical_movement_id, item.reason_code)
        for item in expected_serialization.skipped_items
    }
    declared_skips = {
        (item.canonical_movement_id, item.reason_code)
        for item in serialization_summary.non_serialized_movements
    }
    if expected_skips != declared_skips:
        inconsistencies.append(
            ValidationIssue(
                code="resumo_itens_nao_serializados_divergente",
                severity=ValidationSeverity.FATAL,
                message="Os itens nao serializados declarados no resumo divergem do artefato mapeado.",
            )
        )


def _validate_actual_txt_structure(txt_lines, fatal_errors) -> None:
    for line_number, line in enumerate(txt_lines, start=1):
        try:
            validate_layout_43_structural_fields(line)
        except ValueError as exc:
            fatal_errors.append(
                ValidationIssue(
                    code="txt_linha_estruturalmente_invalida",
                    severity=ValidationSeverity.FATAL,
                    message=str(exc),
                    line_number=line_number,
                )
            )


def _reconcile_txt_content(*, mapped_artifact, expected_serialization, txt_lines, inconsistencies) -> None:
    expected_lines = tuple(item.text for item in expected_serialization.serialized_lines)
    actual_lines = tuple(txt_lines)

    if len(actual_lines) != len(expected_lines):
        inconsistencies.append(
            ValidationIssue(
                code="contagem_txt_divergente",
                severity=ValidationSeverity.FATAL,
                message=(
                    f"O TXT possui {len(actual_lines)} linha(s), mas o serializer reconstituido esperava {len(expected_lines)}."
                ),
            )
        )

    actual_line_set = set(actual_lines)
    expected_line_by_id = {
        item.canonical_movement_id: item.text for item in expected_serialization.serialized_lines
    }
    expected_line_set = set(expected_lines)

    for movement_id, expected_line in expected_line_by_id.items():
        if expected_line not in actual_line_set:
            inconsistencies.append(
                ValidationIssue(
                    code="movimento_pronto_ausente_no_txt",
                    severity=ValidationSeverity.FATAL,
                    message=f"Movimento pronto '{movement_id}' nao apareceu no TXT final.",
                    canonical_movement_id=movement_id,
                )
            )

    for movement in mapped_artifact.artifact.movements:
        skip = evaluate_serialization_eligibility(movement)
        if skip is None:
            continue
        try:
            blocked_line = encode_mapped_movement_to_txt_line(movement)
        except Exception:
            continue
        if blocked_line in actual_line_set:
            inconsistencies.append(
                ValidationIssue(
                    code="movimento_bloqueado_serializado",
                    severity=ValidationSeverity.FATAL,
                    message=(
                        f"Movimento '{movement.canonical_movement_id}' nao era elegivel, mas apareceu no TXT."
                    ),
                    canonical_movement_id=movement.canonical_movement_id,
                )
            )

    for line_number, line in enumerate(actual_lines, start=1):
        if line_number > len(expected_lines):
            if line not in expected_line_set:
                inconsistencies.append(
                    ValidationIssue(
                        code="linha_txt_inesperada",
                        severity=ValidationSeverity.FATAL,
                        message="O TXT contem uma linha inesperada que nao pode ser reconstituida do artefato mapeado.",
                        line_number=line_number,
                    )
                )
            continue

        if line != expected_lines[line_number - 1]:
            inconsistencies.append(
                ValidationIssue(
                    code="conteudo_txt_divergente",
                    severity=ValidationSeverity.FATAL,
                    message=(
                        f"A linha {line_number} do TXT diverge da linha esperada reconstituida do artefato mapeado."
                    ),
                    line_number=line_number,
                )
            )


def _resolve_validation_status(
    *,
    total_mapped_movements: int,
    actual_txt_lines: int,
    fatal_errors,
    inconsistencies,
    warnings,
) -> ValidationStatus:
    if total_mapped_movements == 0 and actual_txt_lines == 0 and not fatal_errors and not inconsistencies:
        return ValidationStatus.EMPTY
    if fatal_errors or inconsistencies:
        return ValidationStatus.BLOCKED
    if warnings:
        return ValidationStatus.SUCCESS_WITH_WARNINGS
    return ValidationStatus.SUCCESS


def _build_recommendation(status: ValidationStatus) -> str:
    if status == ValidationStatus.SUCCESS:
        return "Pronto para seguir para exportacao operacional controlada."
    if status == ValidationStatus.SUCCESS_WITH_WARNINGS:
        return "TXT consistente, mas ha exclusoes ou alertas que exigem revisao antes do uso operacional."
    if status == ValidationStatus.EMPTY:
        return "Nenhuma linha final foi produzida; revisar os bloqueios e os artefatos de entrada."
    return "Nao exportar. Corrigir as inconsistencias e rodar a validacao novamente."


def _build_human_summary(status: ValidationStatus, validation_summary: dict[str, int]) -> str:
    return (
        f"status={status.value} "
        f"fatal={validation_summary['fatal_error_count']} "
        f"inconsistencias={validation_summary['inconsistency_count']} "
        f"warnings={validation_summary['warning_count']} "
        f"linhas_txt={validation_summary['actual_txt_lines']}"
    )

