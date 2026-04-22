# Cadastro Mestre Interno

Este diretorio guarda a base mestre interna de empresas usada pelo `ConfigResolver`.

Arquivos:

- `companies_registry.json`: cadastro mestre das empresas.
- `company_configs.json`: configs internas vinculadas a cada empresa.
- `company_config_issues.json`: pendencias internas do cadastro mestre.

O fluxo oficial de carga inicial e o importador `scripts/import_resumo_mensal.py`.
O repo tambem inclui um seed piloto para o caso Dela More, para manter o fluxo operacional
coerente enquanto a planilha `Resumo Mensal.xls` nao estiver disponivel no workspace.

## Seed em lote da Fase 3B

- O seed em lote usa `default_process=11` por padrao.
- A escolha de `11` segue os goldens do repo e o seed piloto da empresa 72, que ja
  representam o contrato operacional atual da fase V1.
- As configs geradas em lote sao agrupadas por `competence` detectada no cadastro mestre.
- Pendencias resolvidas sao marcadas como `resolved` no arquivo de issues para manter
  trilha historica sem reabrir JSON manual na UI.
