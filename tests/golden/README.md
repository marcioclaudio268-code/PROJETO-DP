# Golden tests

Esta pasta documenta os testes regressivos baseados em golden files.

## Regra

- O teste compara a saida produzida com o artefato dourado correspondente.
- Nenhuma variacao de espaco, largura ou ordem deve ser ignorada.
- Se houver divergencia, o caso precisa ser investigado antes de liberar a mudanca.

## Fluxo esperado

1. Carregar fixture em `data/golden`.
2. Gerar artefato temporario.
3. Comparar byte a byte ou por contrato estrutural.
4. Registrar divergencias em testes de validacao futuros.
