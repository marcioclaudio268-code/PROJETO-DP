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
- `src/ingestion`: loader da planilha humana V1, normalizacao, snapshot canonico, manifesto e persistencia operacional.
- `src/mapping`: consumo do snapshot canonico, carga de configuracao versionada, resolucao deterministica de matricula e rubrica de saida, e persistencia do artefato mapeado.
- `src/serialization`: contrato do layout fixed-width, consumo do artefato mapeado, geracao do TXT de 43 posicoes e persistencia do resumo operacional da serializacao.
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

Ingerir o template V1 e preencher as abas tecnicas:

```bash
python scripts/ingest_planilha_padrao_v1.py --input data/templates/planilha_padrao_folha_v1.xlsx
```

Ingerir e persistir workbook tecnico, snapshot e manifesto:

```bash
python scripts/ingest_planilha_padrao_v1.py --input data/templates/planilha_padrao_folha_v1.xlsx --snapshot-output data/templates/planilha_padrao_folha_v1.snapshot.json
```

Consumir um snapshot persistido e aplicar o mapping por empresa:

```bash
python scripts/map_snapshot_por_empresa.py --snapshot data/templates/planilha_padrao_folha_v1.snapshot.json --config path/para/company_config.json
```

Consumir um artefato mapeado e gerar o TXT fixed-width:

```bash
python scripts/serialize_txt_fixed_width.py --mapped path/para/input.mapped.json
```

## Estado atual da ingestao V1

- O template humano V1 ja existe e e preenchido principalmente em `LANCAMENTOS_FACEIS`.
- A ingestao ja le `PARAMETROS`, `FUNCIONARIOS` e `LANCAMENTOS_FACEIS`.
- A ingestao ja normaliza moeda BR, horas `HH:MM` e quantidades simples.
- Cada linha humana pode gerar multiplos movimentos canonicos em memoria.
- Ambiguidades, conflitos de matricula e eventos nao automatizaveis viram pendencia explicita.
- As abas tecnicas `MOVIMENTOS_CANONICOS` e `PENDENCIAS` ja podem ser atualizadas automaticamente.
- O resultado da ingestao pode ser persistido em snapshot JSON e manifesto minimo.

## Estado atual do mapping por empresa

- O mapping ja consome o snapshot canonico persistido da ingestao.
- A configuracao por empresa e lida de forma versionada via `CompanyConfig`.
- A resolucao de matricula usa o snapshot e `employee_mappings` sem hardcode no dominio.
- O mapeamento `evento_negocio -> rubrica_saida` usa `event_mappings` e nunca inventa rubrica.
- Conflitos, ausencia de mapeamento e ambiguidade relevante viram pendencia explicita de mapping.
- O resultado do mapping e persistido em JSON deterministico, ainda pre-TXT.

## Estado atual do serializer TXT

- O serializer ja consome o artefato intermediario mapeado persistido.
- Apenas movimentos `pronto_para_serializer` entram no TXT.
- O encoder gera linhas fixed-width com exatamente 43 caracteres.
- O serializer nunca trunca silenciosamente campos que excedem largura.
- Movimentos bloqueados ou com dados essenciais ausentes ficam fora do TXT com motivo explicito no resumo JSON.
- O serializer persiste dois artefatos: o `.txt` e um resumo JSON da execucao.

## Fixtures e golden files

O projeto usa fixtures imutaveis para garantir determinismo.

- `data/golden` guarda os artefatos de referencia.
- `tests/golden` guarda a intencao dos testes regressivos e a convencao de comparacao.
- Cada fixture deve ser pequena, nomeada e rastreavel por caso, competencia e versao.
- Nenhuma regra nova entra sem fixture e teste correspondente.

## Proxima etapa esperada

Implementar a validacao final e a reconciliacao ampla dos artefatos gerados antes da exportacao operacional definitiva.
