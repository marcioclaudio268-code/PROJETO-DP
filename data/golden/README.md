# Golden files

Esta pasta guarda fixtures douradas e artefatos de referencia do motor.

## Convencao

- Cada caso deve ficar em uma subpasta propria.
- O caso piloto inicial e `dela_more`.
- Cada fixture deve ter entrada, saida esperada e manifestos suficientes para reproduzir o teste.
- Fixtures nao devem ser editadas silenciosamente; qualquer mudanca de regra exige novo artefato ou nova versao.

## Itens esperados

Exemplo de estrutura futura:

```text
data/golden/
  dela_more/
    2024-03/
      input.xlsx
      expected.txt
      expected_manifest.json
      expected_pending.csv
```

## Regra

Golden files existem para travar determinismo. Se a saida muda, a mudanca precisa ser explicada por spec e teste.
