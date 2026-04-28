# Perfis de Mapeamento de Colunas

Este diretorio guarda perfis JSON por empresa para converter colunas reais de uma
planilha enviada em regras de conversao antes da planilha canonica interna.

O dashboard usa estes perfis para layouts nao canonicos: se o perfil da empresa
nao existir ou nao cobrir as colunas relevantes, a analise bloqueia com pendencia
interna clara antes de gerar o workbook canonico.

Arquivo padrao:

- `<company_code>.json`

Campos principais:

- `company_code`: codigo da empresa.
- `company_name`: nome opcional para leitura operacional.
- `default_process`: processo padrao opcional usado quando o perfil precisar informar esse dado.
- `mappings`: lista de regras por coluna.

Cada mapping informa:

- `column_key` ou `column_name`: identificador da coluna de origem.
- `enabled`: se a regra gera lancamento.
- `rubrica_target`: rubrica unica para `single_line`.
- `rubricas_target`: rubricas multiplas para `multi_line`.
- `value_kind`: `monetario`, `horas` ou `quantidade`.
- `generation_mode`: `single_line`, `multi_line` ou `ignore`.
- `ignore_zero`: ignora valores numericos zerados.
- `ignore_text`: ignora textos como `sim`, `nao` ou observacoes livres.

Exemplos de contrato:

- `GRAT.` -> `20`, `single_line`, `monetario`.
- `ATRASO` -> `8069`, `single_line`, `horas`.
- `FALTA` -> `8792` e `8794`, `multi_line`, `quantidade`.
- `ADIANT. QUINZ` -> `ignore`.

Perfis reais materializados:

- `887.json`: BERBELLA LTDA.
- `1016.json`: MAIS QUE BOLO DOCES E SALGADOS LTDA.
- `448.json`: SAAD E TOSSI LTDA ME.

A empresa `72` permanece como baseline homologada do layout canonico V1 e nao
exige perfil de colunas para o fluxo antigo.
