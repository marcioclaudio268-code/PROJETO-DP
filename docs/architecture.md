# Architecture

## Goal

This repository is the foundation of a deterministic payroll TXT engine for Dominio.

The repository is intentionally small at this stage. It already includes the V1 human spreadsheet template, the ingestion loader, the canonical in-memory model, explicit pendings, workbook technical-tab writing and the persisted ingestion snapshot. The full TXT generation pipeline still does not exist.

## Pipeline

```text
XLSX canonico
  -> ingestion
  -> domain model
  -> mapping por empresa
  -> serialization fixed-width
  -> validation
  -> TXT
```

## Package boundaries

### `src/domain`

Pure domain objects and invariants. No IO, no file system access, no business hardcode by company.

### `src/ingestion`

This package already hosts the V1 human template generator, the XLSX loader for `PARAMETROS`, `FUNCIONARIOS` and `LANCAMENTOS_FACEIS`, reusable normalization helpers, technical-tab writing, the canonical snapshot serializer and the minimum execution manifest.

### `src/mapping`

Company-level resolution for employee registration, event mapping and pending policies. The domain must stay free of company-specific hardcode.

### `src/serialization`

Fixed-width layout metadata and, later, the TXT encoder. The 43-character contract lives here as a structural spec.

### `src/validation`

Layout validation, structural checks, manifest validation and future reconciliation gates.

### `src/config`

Pydantic models for company config, mapping records, pending policy and run manifest.

## Data and tests

- `data/golden` contains immutable fixtures and expected artifacts.
- `tests/golden` documents and hosts regression checks against those fixtures.

## Non-goals for this foundation

- No serializer implementation beyond layout metadata and minimal width checks.
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

## Next implementation slot

The next task should consume the persisted canonical snapshot together with company configuration so the project can move toward deterministic event mapping without jumping yet to TXT serialization.
