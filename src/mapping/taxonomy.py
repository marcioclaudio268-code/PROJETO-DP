"""Minimal taxonomy for deterministic company-level mapping."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from domain import PendingSeverity


class MappingPendingCode(StrEnum):
    EMPLOYEE_MAPPING_MISSING = "mapeamento_matricula_ausente"
    EMPLOYEE_MAPPING_CONFLICT = "mapeamento_matricula_divergente_config"
    EMPLOYEE_MAPPING_AMBIGUOUS = "mapeamento_matricula_ambiguo"
    EVENT_MAPPING_MISSING = "mapeamento_evento_ausente"
    EVENT_MAPPING_INACTIVE = "mapeamento_evento_inativo"
    EVENT_REVIEW_REQUIRED = "evento_requer_revisao_por_politica"


class MappingFatalCode(StrEnum):
    INVALID_COMPANY_CONFIG = "config_empresa_invalida"
    SNAPSHOT_COMPANY_MISMATCH = "snapshot_config_empresa_divergente"
    SNAPSHOT_COMPETENCE_MISMATCH = "snapshot_config_competencia_divergente"


@dataclass(frozen=True, slots=True)
class MappingPendingDefinition:
    code: MappingPendingCode
    severity: PendingSeverity
    description_template: str
    recommended_action_template: str


@dataclass(frozen=True, slots=True)
class MappingFatalDefinition:
    code: MappingFatalCode
    message_template: str


MAPPING_PENDING_CATALOG: dict[MappingPendingCode, MappingPendingDefinition] = {
    MappingPendingCode.EMPLOYEE_MAPPING_MISSING: MappingPendingDefinition(
        code=MappingPendingCode.EMPLOYEE_MAPPING_MISSING,
        severity=PendingSeverity.BLOCKING,
        description_template=(
            "Nao foi possivel resolver a matricula Dominio para a chave '{employee_key_label}' usando o snapshot e a configuracao."
        ),
        recommended_action_template=(
            "Adicionar ou corrigir o employee_mapping da chave '{employee_key_label}' na configuracao da empresa."
        ),
    ),
    MappingPendingCode.EMPLOYEE_MAPPING_CONFLICT: MappingPendingDefinition(
        code=MappingPendingCode.EMPLOYEE_MAPPING_CONFLICT,
        severity=PendingSeverity.BLOCKING,
        description_template=(
            "A matricula do snapshot diverge da configuracao para a chave '{employee_key_label}'. Snapshot='{snapshot_registration}' config='{config_registration}'."
        ),
        recommended_action_template=(
            "Reconciliar a matricula no snapshot e na configuracao antes de seguir para serializer."
        ),
    ),
    MappingPendingCode.EMPLOYEE_MAPPING_AMBIGUOUS: MappingPendingDefinition(
        code=MappingPendingCode.EMPLOYEE_MAPPING_AMBIGUOUS,
        severity=PendingSeverity.BLOCKING,
        description_template=(
            "A configuracao gera mais de uma matricula possivel para a chave '{employee_key_label}': {candidate_registrations}."
        ),
        recommended_action_template=(
            "Remover a ambiguidade em employee_mappings e aliases para a chave '{employee_key_label}'."
        ),
    ),
    MappingPendingCode.EVENT_MAPPING_MISSING: MappingPendingDefinition(
        code=MappingPendingCode.EVENT_MAPPING_MISSING,
        severity=PendingSeverity.BLOCKING,
        description_template="Nao existe rubrica configurada para o evento '{event_name}'.",
        recommended_action_template=(
            "Adicionar o event_mapping de '{event_name}' na configuracao da empresa."
        ),
    ),
    MappingPendingCode.EVENT_MAPPING_INACTIVE: MappingPendingDefinition(
        code=MappingPendingCode.EVENT_MAPPING_INACTIVE,
        severity=PendingSeverity.BLOCKING,
        description_template="O event_mapping de '{event_name}' existe, mas esta inativo.",
        recommended_action_template=(
            "Ativar ou substituir o event_mapping de '{event_name}' antes de seguir."
        ),
    ),
    MappingPendingCode.EVENT_REVIEW_REQUIRED: MappingPendingDefinition(
        code=MappingPendingCode.EVENT_REVIEW_REQUIRED,
        severity=PendingSeverity.HIGH,
        description_template=(
            "O evento '{event_name}' exige revisao humana segundo a pending_policy da empresa."
        ),
        recommended_action_template=(
            "Registrar revisao manual do evento '{event_name}' antes da etapa de serializer."
        ),
    ),
}


MAPPING_FATAL_CATALOG: dict[MappingFatalCode, MappingFatalDefinition] = {
    MappingFatalCode.INVALID_COMPANY_CONFIG: MappingFatalDefinition(
        code=MappingFatalCode.INVALID_COMPANY_CONFIG,
        message_template="Configuracao da empresa invalida: {details}.",
    ),
    MappingFatalCode.SNAPSHOT_COMPANY_MISMATCH: MappingFatalDefinition(
        code=MappingFatalCode.SNAPSHOT_COMPANY_MISMATCH,
        message_template=(
            "Snapshot e configuracao pertencem a empresas diferentes. Snapshot='{snapshot_company_code}' configuracao='{config_company_code}'."
        ),
    ),
    MappingFatalCode.SNAPSHOT_COMPETENCE_MISMATCH: MappingFatalDefinition(
        code=MappingFatalCode.SNAPSHOT_COMPETENCE_MISMATCH,
        message_template=(
            "Snapshot e configuracao pertencem a competencias diferentes. Snapshot='{snapshot_competence}' configuracao='{config_competence}'."
        ),
    ),
}


def render_mapping_pending_definition(code: MappingPendingCode, **context: str) -> tuple[PendingSeverity, str, str]:
    definition = MAPPING_PENDING_CATALOG[code]
    description = definition.description_template.format(**context)
    recommended_action = definition.recommended_action_template.format(**context)
    return definition.severity, description, recommended_action


def render_mapping_fatal_message(code: MappingFatalCode, **context: str) -> str:
    definition = MAPPING_FATAL_CATALOG[code]
    return definition.message_template.format(**context)
