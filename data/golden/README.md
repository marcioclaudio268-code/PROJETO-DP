# Golden files

Esta pasta guarda fixtures douradas e artefatos de referencia do motor.

## Convencao

- Cada caso V1 deve ficar em `data/golden/v1/<nome_do_caso>/`.
- Cada caso deve trazer entrada humana e saidas persistidas do pipeline completo.
- O caso deve ser pequeno, rastreavel e estavel para diff.
- Fixtures nao devem ser editadas silenciosamente; qualquer mudanca de regra exige novo artefato ou nova versao.

## Itens esperados

Estrutura esperada:

```text
data/golden/
  v1/
    happy_path/
      input.xlsx
      company_config.json
      expected.snapshot.json
      expected.mapped.json
      expected.txt
      expected.serialization.json
      expected.validation.json
    warning_exclusion/
      input.xlsx
      company_config.json
      expected.snapshot.json
      expected.mapped.json
      expected.txt
      expected.serialization.json
      expected.validation.json
```

## Regra

Golden files existem para travar determinismo. Se a saida muda, a mudanca precisa ser explicada por spec e teste.
