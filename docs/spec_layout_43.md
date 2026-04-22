# Spec do layout TXT de 43 posicoes

## Status

Fechado operacionalmente para o serializer V1, com assuncoes explicitas documentadas abaixo.

## Contrato fisico

O layout e fixo e tem a seguinte divisao:

`1 + 11 + 6 + 4 + 2 + 9 + 10 = 43`

## Campos semanticos do serializer

| Campo | Inicio | Fim | Tamanho | Tipo | Padding | Regra de preenchimento |
| --- | ---: | ---: | ---: | --- | --- | --- |
| `tipo_registro` | 1 | 1 | 1 | literal | sem padding | Literal fixo `1` para linha de lancamento do V1. |
| `matricula_dominio` | 2 | 12 | 11 | numerico | zeros a esquerda | Matricula final resolvida no mapping. Deve conter apenas digitos e caber em 11. |
| `rubrica_saida` | 13 | 18 | 6 | numerico | zeros a esquerda | Rubrica resolvida no mapping. Deve conter apenas digitos e caber em 6. |
| `codigo_empresa` | 19 | 22 | 4 | numerico | zeros a esquerda | Codigo da empresa serializado por linha. Deve conter apenas digitos e caber em 4. |
| `codigo_processo` | 23 | 24 | 2 | numerico | zeros a esquerda | Processo padrao do movimento. O serializer V1 aceita no maximo 2 digitos. |
| `referencia` | 25 | 33 | 9 | numerico dependente do tipo | zeros a esquerda | Monetario usa zeros. Horas usam `00000HHMM`. Dias usam 2 casas decimais implicitas. |
| `valor` | 34 | 43 | 10 | numerico com 2 casas implicitas | zeros a esquerda | Monetario usa o valor do movimento. Horas e dias usam zeros. |

## Regras operacionais do V1

- Cada linha deve ter exatamente 43 caracteres.
- O serializer nunca trunca silenciosamente.
- Campos identificadores (`matricula_dominio`, `rubrica_saida`, `codigo_empresa`, `codigo_processo`) devem conter apenas digitos.
- Campo com valor ausente, nao numerico ou acima da largura reservada gera erro explicito no encoder e exclusao controlada no resumo da serializacao.
- A ordem das linhas no TXT preserva a ordem dos movimentos no artefato mapeado.

## Elegibilidade para serializacao

Um movimento so vira linha TXT quando todas as condicoes abaixo forem verdadeiras:

- `status = pronto_para_serializer`
- `resolved_domain_registration` preenchida
- `output_rubric` preenchida
- os campos necessarios cabem na largura do layout
- o payload do valor do movimento e compativel com o tipo do movimento

Casos que nao serializam:

- movimento bloqueado
- movimento sem matricula final resolvida
- movimento sem rubrica de saida resolvida
- identificador numerico invalido
- campo que excede largura
- payload de quantidade/horas/valor invalido para o encoder

Esses casos nao entram no TXT e aparecem no resumo JSON da serializacao com motivo explicito.

## Regra por tipo de movimento

### Movimento monetario

- `referencia` = `000000000`
- `valor` = valor monetario com 2 casas decimais implicitas

Exemplo:

- valor do movimento: `100,00`
- campo `valor`: `0000010000`

### Movimento em horas

- `referencia` = horas em formato `HHMM`, sem separador, alinhadas a direita no campo de 9 posicoes
- `valor` = `0000000000`

Exemplo:

- horas do movimento: `02:16`
- campo `referencia`: `000000216`

### Movimento em dias

- `referencia` = quantidade com 2 casas decimais implicitas
- `valor` = `0000000000`

Exemplo:

- quantidade: `3`
- campo `referencia`: `000000300`

## Exemplos completos de linha

### Exemplo monetario

Valores:

- `tipo_registro = 1`
- `matricula_dominio = 123`
- `rubrica_saida = 201`
- `codigo_empresa = 72`
- `codigo_processo = 11`
- `referencia = 000000000`
- `valor = 0000010000`

Linha:

```text
1000000001230002010072110000000000000010000
```

### Exemplo em horas

Valores:

- `tipo_registro = 1`
- `matricula_dominio = 123`
- `rubrica_saida = 350`
- `codigo_empresa = 72`
- `codigo_processo = 11`
- `referencia = 000000216`
- `valor = 0000000000`

Linha:

```text
1000000001230003500072110000002160000000000
```

## Assuncoes explicitas do serializer V1

- O layout de linha do V1 nao carrega `competencia`; ela permanece no artefato mapeado e no resumo da serializacao.
- O campo `codigo_empresa` por linha e uma assuncao operacional do V1 para fechar o layout fisico sem hardcode no dominio.
- O campo `codigo_processo` e serializado com largura `2`. Se a configuracao usar valor maior, o movimento nao e truncado; ele fica fora do TXT com motivo explicito.
- Movimentos de horas e dias saem com `valor = 0`, porque o artefato mapeado atual nao carrega valor monetario calculado para essas naturezas.
- O serializer assume que a natureza financeira final do evento ja esta embutida na `rubrica_saida` resolvida no mapping. Ele nao injeta sinal ou regra fiscal adicional.

## Comportamento quando o movimento nao puder ser serializado

- O movimento nao gera linha TXT.
- O resumo JSON registra o `canonical_movement_id`, o motivo da exclusao e uma mensagem legivel.
- O status final da serializacao fica:
  - `success` quando tudo serializa
  - `success_with_exclusions` quando parte serializa e parte fica de fora
  - `blocked` quando nada serializa
  - `empty` quando nao ha movimentos no artefato de entrada
