# Architecture

## Goal

This repository is the foundation of a deterministic payroll TXT engine for Dominio.

The repository is intentionally small at this stage. It already includes the V1 human spreadsheet template, the ingestion loader, the canonical in-memory model, explicit pendings, workbook technical-tab writing, the persisted ingestion snapshot, the deterministic company-level mapping stage, the fixed-width TXT serializer and the final validation/reconciliation artifact.

## Pipeline

```text
XLSX canonico
  -> ingestion
  -> domain model
  -> mapping por empresa
  -> artefato mapeado pre-TXT
  -> serialization fixed-width
  -> TXT pre-validacao
  -> validation
  -> TXT
```

## Package boundaries

### `src/domain`

Pure domain objects and invariants. No IO, no file system access, no business hardcode by company.

### `src/ingestion`

This package already hosts the V1 human template generator, the XLSX loader for `PARAMETROS`, `FUNCIONARIOS` and `LANCAMENTOS_FACEIS`, reusable normalization helpers, technical-tab writing, the canonical snapshot serializer and the minimum execution manifest.

### `src/mapping`

Company-level resolution for employee registration, event mapping and pending policies. This package consumes the persisted canonical snapshot plus `CompanyConfig`, produces a mapped intermediate artifact and keeps the domain free of company-specific hardcode.

### `src/serialization`

Fixed-width layout metadata, mapped-artifact loading, the TXT encoder and persistence of the `.txt` plus a serialization summary JSON. The 43-character contract lives here as executable spec.

### `src/validation`

Layout validation, structural checks, TXT reading, serialization-summary loading, cross-artifact reconciliation and persistence of the final validation artifact.

### `src/dashboard`

Local operational UI support. This package orchestrates the existing pipeline, resolves the internal company config automatically, persists execution-local state, applies guided overrides on top of editable workbook/config copies and decides when the TXT download can be released safely. The automatic config lookup is isolated behind `ConfigResolver`, which checks the internal master data first and falls back to `configs/companies` only when needed.

### `src/config`

Pydantic models for company config, master registry records, config issue records, mapping records, pending policy and run manifest. This package also hosts the JSON-backed internal master-data store and the `Resumo Mensal.xls` import helper.

## Data and tests

- `data/golden` contains immutable fixtures and expected artifacts.
- `tests/golden` documents and hosts regression checks against those fixtures.
- The V1 golden convention is one folder per case with human input, company config and all persisted artifacts from ingestion, mapping, serialization and final validation.

## Non-goals for this foundation

- No new serializer layout beyond the implemented 43-character V1 contract.
- No adiantamento, PLR, ponto, ferias, rescisao or CNAB.
- No remote, multiuser or standalone web UI beyond the implemented local dashboard.
- No silent AI decision making in production.

## Current ingestion state

The current ingestion flow is:

```text
planilha_padrao_folha_v1.xlsx
  -> leitura validada de PARAMETROS / FUNCIONARIOS / LANCAMENTOS_FACEIS
  -> normalizacao deterministica
  -> movimentos canonicos em memoria
  -> pendencias explicitas
  -> escrita de MOVIMENTOS_CANONICOS / PENDENCIAS
  -> snapshot JSON canonico
  -> manifesto minimo de execucao
```

## Current mapping state

The current mapping flow is:

```text
snapshot JSON canonico
  + configuracao versionada por empresa
  -> resolucao deterministica de matricula
  -> resolucao deterministica de rubrica_saida
  -> pendencias explicitas de mapping
  -> artefato JSON mapeado pre-TXT
```

## Current serialization state

The current serialization flow is:

```text
artefato JSON mapeado
  -> filtro deterministico de elegibilidade
  -> encoder fixed-width de 43 posicoes
  -> arquivo TXT
  -> resumo JSON da serializacao
```

## Current final validation state

The current validation flow is:

```text
snapshot canonico
  + artefato mapeado
  + resumo JSON da serializacao
  + TXT gerado
  -> reconciliacao deterministica entre etapas
  -> validacao estrutural do TXT
  -> artefato JSON final de validacao
```

## Current dashboard state

The current dashboard flow is:

```text
planilha
  -> copia local editavel da execucao
  -> deteccao de empresa e competencia
  -> resolucao interna de configuracao por empresa
  -> analise guiada sobre o pipeline V1 existente
  -> lista de pendencias compreensivel para o escritorio
  -> correcao guiada / ignorar nesta importacao com rastreabilidade
  -> reprocessamento
  -> liberacao controlada do TXT
```

The dashboard resolver now checks the internal company master data first. The priority is:

```text
registry + specific config
  -> registry + active config
  -> legacy file fallback
  -> internal pending
```

The master data itself is stored under `data/company_master` and can be refreshed with `scripts/import_resumo_mensal.py --input /caminho/Resumo Mensal.xls`.

## Next implementation slot

The V1 backbone is now complete and wrapped by a local dashboard. The next work should focus on operational refinement: better guided correction coverage, stronger regression checks and narrower UX improvements without rewriting the core pipeline.
