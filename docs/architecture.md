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

### `src/config`

Pydantic models for company config, mapping records, pending policy and run manifest.

## Data and tests

- `data/golden` contains immutable fixtures and expected artifacts.
- `tests/golden` documents and hosts regression checks against those fixtures.
- The V1 golden convention is one folder per case with human input, company config and all persisted artifacts from ingestion, mapping, serialization and final validation.

## Non-goals for this foundation

- No new serializer layout beyond the implemented 43-character V1 contract.
- No adiantamento, PLR, ponto, ferias, rescisao or CNAB.
- No web UI.
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

## Next implementation slot

The V1 backbone is now complete. The next work should focus on hardening: stronger golden coverage, end-to-end regression checks and small reliability fixes without rewriting the core pipeline.
