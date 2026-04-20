# Architecture

## Goal

This repository is the foundation of a deterministic payroll TXT engine for Dominio.

The repository is intentionally small at this stage. It defines contracts, package boundaries, validation hooks and fixture conventions before the full generation pipeline exists.

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

Future XLSX loader for the canonical spreadsheet. This is where number and hour normalization will enter in the next task.

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

## Next implementation slot

The next task should add:

1. Canonical XLSX loader.
2. Normalization of Brazilian numbers.
3. Normalization of hours.
4. Canonical in-memory record snapshot.
