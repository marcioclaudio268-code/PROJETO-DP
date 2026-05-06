"""Fixed-width layout metadata for the Dominio TXT import."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class LayoutFieldSpec:
    """Operational metadata for one fixed-width field."""

    name: str
    width: int
    type_hint: str
    padding: str
    description: str


LAYOUT_43_FIELDS: tuple[LayoutFieldSpec, ...] = (
    LayoutFieldSpec(
        name="tipo_registro",
        width=1,
        type_hint="literal",
        padding="sem padding",
        description="Marcador fixo da linha de lancamento. V1 usa o literal '1'.",
    ),
    LayoutFieldSpec(
        name="matricula_dominio",
        width=11,
        type_hint="numerico",
        padding="zeros a esquerda",
        description="Matricula final resolvida do colaborador no Dominio.",
    ),
    LayoutFieldSpec(
        name="competencia",
        width=6,
        type_hint="numerico",
        padding="zeros a esquerda",
        description="Competencia final no formato AAAAMM.",
    ),
    LayoutFieldSpec(
        name="rubrica_saida",
        width=4,
        type_hint="numerico",
        padding="zeros a esquerda",
        description="Rubrica de saida resolvida no mapping por empresa.",
    ),
    LayoutFieldSpec(
        name="codigo_processo",
        width=2,
        type_hint="numerico",
        padding="zeros a esquerda",
        description="Processo padrao da execucao, restrito a 2 digitos no serializer V1.",
    ),
    LayoutFieldSpec(
        name="valor_ou_referencia",
        width=9,
        type_hint="numerico_dependente_do_tipo",
        padding="zeros a esquerda",
        description=(
            "Campo unificado do Dominio. Monetario usa valor com 2 casas implicitas; horas usam HHMM; dias/quantidades usam 2 casas implicitas."
        ),
    ),
    LayoutFieldSpec(
        name="codigo_empresa",
        width=10,
        type_hint="numerico",
        padding="zeros a esquerda",
        description="Codigo da empresa serializado por linha no layout final do Dominio.",
    ),
)

LAYOUT_43_TOTAL_WIDTH: int = sum(field.width for field in LAYOUT_43_FIELDS)


def layout_43_widths() -> tuple[int, ...]:
    """Return the tuple of widths that define the layout."""

    return tuple(field.width for field in LAYOUT_43_FIELDS)


def layout_43_field_names() -> tuple[str, ...]:
    """Return the ordered field names used by the current structural spec."""

    return tuple(field.name for field in LAYOUT_43_FIELDS)
