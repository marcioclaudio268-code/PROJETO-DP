# Cadastro Mestre Interno

Este diretorio guarda a base mestre interna de empresas usada pelo `ConfigResolver`.

Arquivos:

- `companies_registry.json`: cadastro mestre das empresas.
- `company_configs.json`: configs internas vinculadas a cada empresa.
- `company_config_issues.json`: pendencias internas do cadastro mestre.

O fluxo oficial de carga inicial e o importador `scripts/import_resumo_mensal.py`.
O repo tambem inclui um seed piloto para o caso Dela More, para manter o fluxo operacional
coerente enquanto a planilha `Resumo Mensal.xls` nao estiver disponivel no workspace.
