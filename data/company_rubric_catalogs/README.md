# Catalogos persistentes de rubricas por empresa

Este diretorio armazena catalogos JSON simples usados pelo backend do dashboard para reaproveitar rubricas Dominio entre importacoes.

Formato por empresa: `<company_code>.json`.

Campos principais:
- `company_code`
- `company_name`
- `rubrics`
- `rubric_code`
- `description`
- `canonical_event`
- `value_kind`
- `nature`
- `aliases`
- `status`
- `source`
- `notes`
- `updated_at`

A gravacao permanente por acao manual exige `persist_to_rubric_catalog: true` no payload da acao.
