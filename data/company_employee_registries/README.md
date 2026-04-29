# Cadastros persistentes de funcionarios por empresa

Este diretorio armazena cadastros JSON simples usados pelo backend do dashboard para reaproveitar matriculas Dominio entre importacoes.

Formato por empresa: `<company_code>.json`.

Campos principais:
- `company_code`
- `company_name`
- `employees`
- `employee_key`
- `employee_name`
- `domain_registration`
- `aliases`
- `status`
- `source`
- `notes`
- `updated_at`

A gravacao permanente por acao manual exige `persist_to_employee_registry: true` no payload da acao.
