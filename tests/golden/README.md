# Golden tests

Esta pasta documenta os testes regressivos baseados em golden files.

## Regra

- O teste compara a saida produzida com o artefato dourado correspondente.
- Nenhuma variacao de espaco, largura ou ordem deve ser ignorada.
- Campos de caminho absolutos e o hash do resumo de serializacao dentro do artefato final de validacao sao normalizados por serem dependentes do diretorio temporario do teste.
- Se houver divergencia, o caso precisa ser investigado antes de liberar a mudanca.

## Fluxo esperado

1. Carregar fixture em `data/golden`.
2. Copiar a fixture para um diretorio temporario mantendo nomes previsiveis.
3. Executar ingestao, snapshot, mapping, serializer e validacao final.
4. Comparar TXT byte a byte e comparar JSONs apos normalizacao minima dos campos volateis.
