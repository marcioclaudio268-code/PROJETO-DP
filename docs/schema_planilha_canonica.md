# Schema da planilha canonica

## Proposito

A planilha canonica e a entrada corrigida para o motor. Ela nao e a planilha baguncada e nao deve carregar regra silenciosa de negocio.

## Contrato inicial

| Campo | Obrigatorio | Tipo inicial | Descricao | Status |
| --- | --- | --- | --- | --- |
| empresa_filial | Sim | string | Identificador da empresa e/ou filial da linha. A forma exata do codigo e ABERTO. | Fechado estruturalmente |
| competencia | Sim | string | Competencia da linha no formato `MM/AAAA` no V1. | Fechado estruturalmente |
| colaborador | Sim | string | Identificador legivel do colaborador na origem. | Fechado estruturalmente |
| matricula_dominio | Condicional | string | Matricula no Dominio. Quando nao existir, a linha deve usar uma chave de resolucao. | ABERTO |
| chave_resolucao | Condicional | string | Chave de resolucao para chegar na matricula Dominio. | ABERTO |
| evento_negocio | Sim | string | Evento de negocio canonico, desacoplado da rubrica de saida. | Fechado estruturalmente |
| quantidade | Condicional | decimal | Medida numerica quando o evento usar quantidade. | ABERTO |
| horas | Condicional | hora | Medida de horas quando o evento usar horas. | ABERTO |
| valor | Condicional | decimal BR | Medida monetaria quando o evento usar valor. | ABERTO |
| observacoes | Nao | string | Observacoes da origem. Nao podem ser exportadas automaticamente se estiverem ambiguas. | ABERTO |
| origem_linha | Sim | inteiro | Numero da linha de origem na planilha. | Fechado estruturalmente |
| origem_celula | Sim | string | Referencia da celula de origem para auditoria. | Fechado estruturalmente |
| pendencia | Nao | string | Codigo ou texto de pendencia. Formato final ABERTO. | ABERTO |

## Regras minimas

- Uma linha canonica representa um fato de folha que sera avaliado pelo motor.
- O loader do V1 nao deve inferir regra de negocio a partir de observacoes textuais ambiguas.
- `matricula_dominio` e `chave_resolucao` nao devem ser tratados como hardcode por empresa.
- A origem da linha e da celula precisa ser preservada para auditoria.
- As colunas `quantidade`, `horas` e `valor` dependem do catalogo de eventos canonicos e ja sao tratadas de forma deterministica no V1 para moeda, horas e dias.

## Campos obrigatorios vs opcionais

- Obrigatorios: `empresa_filial`, `competencia`, `colaborador`, `evento_negocio`, `origem_linha`, `origem_celula`.
- Condicionais: `matricula_dominio` e `chave_resolucao`.
- Condicionais: `quantidade`, `horas`, `valor`.
- Opcionais: `observacoes`, `pendencia`.

## Pendencias abertas

- Regra de exclusividade entre `matricula_dominio` e `chave_resolucao`.
- Regra de exclusividade entre `quantidade`, `horas` e `valor`.
