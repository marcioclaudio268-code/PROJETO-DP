"""Formal taxonomy for V1 ingestion pendings and fatal structural errors."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from domain import PendingCode, PendingSeverity


class FatalIngestionCode(StrEnum):
    MISSING_REQUIRED_SHEET = "aba_obrigatoria_ausente"
    MISSING_REQUIRED_HEADER = "cabecalho_obrigatorio_ausente"
    PARAMETER_REQUIRED_MISSING = "parametro_obrigatorio_ausente"
    COMPETENCE_INVALID = "competencia_invalida"
    PAYROLL_TYPE_NOT_SUPPORTED = "tipo_folha_nao_suportado"
    LAYOUT_VERSION_NOT_SUPPORTED = "versao_layout_nao_suportada"


@dataclass(frozen=True, slots=True)
class PendingDefinition:
    code: PendingCode
    severity: PendingSeverity
    description_template: str
    recommended_action_template: str


@dataclass(frozen=True, slots=True)
class FatalErrorDefinition:
    code: FatalIngestionCode
    message_template: str


PENDING_CATALOG: dict[PendingCode, PendingDefinition] = {
    PendingCode.PARAMETER_REQUIRED_MISSING: PendingDefinition(
        code=PendingCode.PARAMETER_REQUIRED_MISSING,
        severity=PendingSeverity.BLOCKING,
        description_template="Parametro obrigatorio ausente: {field_name}.",
        recommended_action_template="Preencher o parametro '{field_name}' na aba PARAMETROS.",
    ),
    PendingCode.EMPLOYEE_NOT_FOUND: PendingDefinition(
        code=PendingCode.EMPLOYEE_NOT_FOUND,
        severity=PendingSeverity.BLOCKING,
        description_template="Nao foi possivel localizar o colaborador no cadastro auxiliar de FUNCIONARIOS.",
        recommended_action_template="Preencher ou corrigir 'chave_colaborador' e revisar o cadastro auxiliar.",
    ),
    PendingCode.DOMAIN_REGISTRATION_MISSING: PendingDefinition(
        code=PendingCode.DOMAIN_REGISTRATION_MISSING,
        severity=PendingSeverity.BLOCKING,
        description_template="A matricula Dominio esta ausente para a linha preenchida.",
        recommended_action_template="Informar a matricula Dominio no cadastro ou na propria linha antes de seguir.",
    ),
    PendingCode.DOMAIN_REGISTRATION_DUPLICATED: PendingDefinition(
        code=PendingCode.DOMAIN_REGISTRATION_DUPLICATED,
        severity=PendingSeverity.HIGH,
        description_template="A matricula Dominio aparece mais de uma vez na aba FUNCIONARIOS.",
        recommended_action_template="Revisar o cadastro auxiliar e manter a matricula vinculada a apenas um colaborador.",
    ),
    PendingCode.DOMAIN_REGISTRATION_LINE_ONLY: PendingDefinition(
        code=PendingCode.DOMAIN_REGISTRATION_LINE_ONLY,
        severity=PendingSeverity.HIGH,
        description_template="A matricula Dominio foi informada na linha, mas esta ausente no cadastro auxiliar.",
        recommended_action_template="Atualizar o cadastro auxiliar ou confirmar explicitamente a matricula informada na linha.",
    ),
    PendingCode.DOMAIN_REGISTRATION_CONFLICT: PendingDefinition(
        code=PendingCode.DOMAIN_REGISTRATION_CONFLICT,
        severity=PendingSeverity.BLOCKING,
        description_template="A matricula Dominio informada na linha diverge do cadastro auxiliar.",
        recommended_action_template="Reconciliar a matricula entre FUNCIONARIOS e LANCAMENTOS_FACEIS antes de seguir.",
    ),
    PendingCode.EMPLOYEE_REGISTRY_INCONSISTENT: PendingDefinition(
        code=PendingCode.EMPLOYEE_REGISTRY_INCONSISTENT,
        severity=PendingSeverity.BLOCKING,
        description_template="O cadastro auxiliar do colaborador esta inconsistente para a chave informada.",
        recommended_action_template="Revisar duplicidades ou conflitos na aba FUNCIONARIOS antes de prosseguir.",
    ),
    PendingCode.INVALID_HOUR: PendingDefinition(
        code=PendingCode.INVALID_HOUR,
        severity=PendingSeverity.BLOCKING,
        description_template="Hora invalida. Use o formato HH:MM.",
        recommended_action_template="Corrigir o valor de horas na aba LANCAMENTOS_FACEIS.",
    ),
    PendingCode.INVALID_VALUE: PendingDefinition(
        code=PendingCode.INVALID_VALUE,
        severity=PendingSeverity.BLOCKING,
        description_template="Valor monetario invalido para a coluna informada.",
        recommended_action_template="Corrigir o valor monetario na aba LANCAMENTOS_FACEIS.",
    ),
    PendingCode.INVALID_QUANTITY: PendingDefinition(
        code=PendingCode.INVALID_QUANTITY,
        severity=PendingSeverity.BLOCKING,
        description_template="Quantidade invalida para a coluna informada.",
        recommended_action_template="Corrigir a quantidade na aba LANCAMENTOS_FACEIS.",
    ),
    PendingCode.AMBIGUOUS_EVENT_NOTE: PendingDefinition(
        code=PendingCode.AMBIGUOUS_EVENT_NOTE,
        severity=PendingSeverity.HIGH,
        description_template="A coluna 'observacao_eventos' foi preenchida e exige revisao humana.",
        recommended_action_template="Interpretar a observacao manualmente e ajustar os eventos explicitamente, sem inferencia automatica.",
    ),
    PendingCode.NON_AUTOMATABLE_EVENT: PendingDefinition(
        code=PendingCode.NON_AUTOMATABLE_EVENT,
        severity=PendingSeverity.MEDIUM,
        description_template="O evento '{event_name}' nao e automatizado nesta etapa e nao gerou movimento canonico.",
        recommended_action_template="Avaliar manualmente este evento e decidir o tratamento antes de exportar.",
    ),
    PendingCode.LINE_BLOCKED_BY_STATUS: PendingDefinition(
        code=PendingCode.LINE_BLOCKED_BY_STATUS,
        severity=PendingSeverity.HIGH,
        description_template="Linha bloqueada pelo cadastro de funcionarios. 'admite_lancamento' esta como 'nao' ou o status do colaborador impede processamento.",
        recommended_action_template="Revisar o cadastro em FUNCIONARIOS e liberar a linha somente se o lancamento for permitido.",
    ),
}


FATAL_ERROR_CATALOG: dict[FatalIngestionCode, FatalErrorDefinition] = {
    FatalIngestionCode.MISSING_REQUIRED_SHEET: FatalErrorDefinition(
        code=FatalIngestionCode.MISSING_REQUIRED_SHEET,
        message_template="Workbook nao contem todas as abas obrigatorias. Ausentes: {missing}.",
    ),
    FatalIngestionCode.MISSING_REQUIRED_HEADER: FatalErrorDefinition(
        code=FatalIngestionCode.MISSING_REQUIRED_HEADER,
        message_template="Aba '{sheet_name}' nao contem todos os cabecalhos esperados. Ausentes: {missing}.",
    ),
    FatalIngestionCode.PARAMETER_REQUIRED_MISSING: FatalErrorDefinition(
        code=FatalIngestionCode.PARAMETER_REQUIRED_MISSING,
        message_template="Parametro obrigatorio ausente: {field_name}.",
    ),
    FatalIngestionCode.COMPETENCE_INVALID: FatalErrorDefinition(
        code=FatalIngestionCode.COMPETENCE_INVALID,
        message_template="Competencia invalida. Use o formato MM/AAAA.",
    ),
    FatalIngestionCode.PAYROLL_TYPE_NOT_SUPPORTED: FatalErrorDefinition(
        code=FatalIngestionCode.PAYROLL_TYPE_NOT_SUPPORTED,
        message_template="tipo_folha '{payroll_type}' nao e suportado nesta fase. Use apenas 'mensal'.",
    ),
    FatalIngestionCode.LAYOUT_VERSION_NOT_SUPPORTED: FatalErrorDefinition(
        code=FatalIngestionCode.LAYOUT_VERSION_NOT_SUPPORTED,
        message_template="versao_layout '{layout_version}' nao e suportada. Esta ingestao aceita apenas '{supported_layout_version}'.",
    ),
}


def render_pending_definition(code: PendingCode, **context: str) -> tuple[PendingSeverity, str, str]:
    definition = PENDING_CATALOG[code]
    description = definition.description_template.format(**context)
    recommended_action = definition.recommended_action_template.format(**context)
    return definition.severity, description, recommended_action


def render_fatal_error_message(code: FatalIngestionCode, **context: str) -> str:
    definition = FATAL_ERROR_CATALOG[code]
    return definition.message_template.format(**context)
