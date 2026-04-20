# motor-txt-dominio-folha

Motor seguro e deterministico para gerar TXT de importacao da folha no Dominio a partir de uma planilha canonica corrigida em XLSX.

## Objetivo

O produto existe para transformar uma entrada canonicamente corrigida em uma saida TXT fixed-width, sem decisao silenciosa de IA na linha final do arquivo.

Fluxo alvo:

`planilha canonica -> modelo interno -> mapeamento por empresa -> serializer fixed-width -> validacao -> TXT`

## Escopo V1

- Folha mensal / pagamento.
- Caso piloto inicial: Dela More.
- Base para evoluir com seguranca para novos casos.

## Fora de escopo nesta fase

- Adiantamento.
- PLR.
- Ponto.
- Ferias.
- Rescisao.
- CNAB bancario.
- Interface web.
- Normalizador da planilha baguncada.

## Principios

- O core do motor e deterministico.
- Nao existe hardcode por empresa no dominio.
- IA pode apoiar desenvolvimento e saneamento assistido, mas nao decide silenciosamente a linha final do TXT em producao.
- Qualquer ambiguidade relevante vira pendencia explicita.
- Funcoes puras e codigo testavel sao a regra.

## Estrutura

- `src/domain`: objetos puros e invariantes do dominio.
- `src/ingestion`: futuro loader da planilha canonica XLSX.
- `src/mapping`: resolucao por empresa, matricula e rubrica.
- `src/serialization`: contrato de layout fixed-width e futuro serializer.
- `src/validation`: validacoes estruturais, layout e reconciliacao futura.
- `src/config`: modelos Pydantic para configuracao por empresa e manifestos de execucao.
- `data/templates`: templates XLSX versionados para preenchimento humano.
- `data/golden`: fixtures douradas e artefatos de referencia.
- `tests/golden`: testes de regressao baseados em golden files.

## Como rodar

Criar ambiente e instalar dependencias:

```bash
python -m pip install -e .[dev]
```

Rodar testes:

```bash
pytest
```

Gerar o template Excel V1:

```bash
python scripts/generate_planilha_padrao_v1.py
```

## Fixtures e golden files

O projeto usa fixtures imutaveis para garantir determinismo.

- `data/golden` guarda os artefatos de referencia.
- `tests/golden` guarda a intencao dos testes regressivos e a convencao de comparacao.
- Cada fixture deve ser pequena, nomeada e rastreavel por caso, competencia e versao.
- Nenhuma regra nova entra sem fixture e teste correspondente.

## Proxima etapa esperada

Implementar o loader da planilha canonica e o normalizador de numeros e horas, sem iniciar o serializer completo.
