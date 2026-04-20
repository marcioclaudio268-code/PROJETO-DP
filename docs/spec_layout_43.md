# Spec do layout TXT de 43 posicoes

## Status

ABERTO em nivel semantico. O contrato fisico esta fechado em 43 caracteres, mas os nomes funcionais finais dos 7 campos ainda nao foram formalizados neste passo.

## Contrato fisico

O layout e fixo e tem a seguinte divisao:

`1 + 11 + 6 + 4 + 2 + 9 + 10 = 43`

### Campos fisicos

| Campo | Inicio | Fim | Tamanho | Tipo esperado | Padding | Status |
| --- | ---: | ---: | ---: | --- | --- | --- |
| campo_01 | 1 | 1 | 1 | ABERTO | ABERTO | ABERTO |
| campo_02 | 2 | 12 | 11 | ABERTO | ABERTO | ABERTO |
| campo_03 | 13 | 18 | 6 | ABERTO | ABERTO | ABERTO |
| campo_04 | 19 | 22 | 4 | ABERTO | ABERTO | ABERTO |
| campo_05 | 23 | 24 | 2 | ABERTO | ABERTO | ABERTO |
| campo_06 | 25 | 33 | 9 | ABERTO | ABERTO | ABERTO |
| campo_07 | 34 | 43 | 10 | ABERTO | ABERTO | ABERTO |

## Regras estruturais minimas

- Cada linha deve ter exatamente 43 caracteres.
- O serializer nao pode truncar silenciosamente campos maiores que a largura reservada.
- Qualquer semantica nao fechada permanece marcada como ABERTO.
- Nenhuma regra de negocio e assumida apenas pela posicao do campo.

## Exemplos observados para teste humano

Os exemplos abaixo existem como referencias de dominio, mas o formato final de serializacao continua ABERTO nesta etapa:

- Valor monetario observado: `293,08`
- Hora observada: `02:16`

## Validacoes ainda pendentes

- Nome funcional definitivo de cada um dos 7 campos.
- Tipo final de cada campo.
- Padding final por tipo.
- Regra de optionalidade por campo.
- Regra de preenchimento numerico e de horas.
- Regra de normalizacao de valores textuais ambiguos.

## Nota de seguranca

Observacoes textuais ambiguas nao podem ser exportadas automaticamente sem regra fechada ou pendencia explicita.
