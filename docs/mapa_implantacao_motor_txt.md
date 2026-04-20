# Mapa de implantação do motor seguro de geração de TXT para o Domínio

## Decisão arquitetural

- O **core do produto** será um motor **determinístico**: planilha canônica -> modelo interno -> mapeamento por empresa -> serializer fixed-width -> validação -> TXT.
- **IA entra na construção do programa e em saneamento assistido**, não como autora silenciosa da linha final do TXT em produção.
- O **escopo V1** será **Folha Mensal / Pagamento**. Adiantamento, PLR, ponto, férias, rescisão e CNAB bancário ficam em módulos separados.
- O **layout-alvo de importação do Domínio** será modelado a partir das amostras externas de importação já coletadas: **1 + 11 + 6 + 4 + 2 + 9 + 10 = 43 posições**.
- Correção crítica do inventário: os **5 TXTs do acervo BTC não são gabaritos de importação do Domínio**; eles são **arquivos CNAB/remessa bancária de 240 posições** e devem virar um **módulo futuro separado**.

## O que entra no produto

### V1 - Motor de importação da folha mensal

1. **Entrada canônica**: planilha corrigida em XLSX.
2. **Configuração versionada por empresa/filial**: processo, rubricas de saída, matrícula Domínio e exceções.
3. **Serializer TXT de 43 posições**.
4. **Validador estrutural**.
5. **Reconciliação agregada** contra Resumo Folha Pagamento.
6. **Auditoria amostral** contra Folha Pagamento/Holerites.
7. **Fila de pendências** para campos ambíguos.

### V2

- Adiantamento.
- Auditoria fiscal com DAE/GFD/RE GFD.
- Whitelist útil dos arquivos “outros”.
- Interface web local.

### V3

- Normalizador da planilha bagunçada.
- Normalizador de ponto.
- PLR.
- CNAB/remessa bancária.
- Férias/rescisão.

## Mapa das fontes documentais

| ativo                              |   qtd | classe           | uso_no_produto                                                                                        | modulo_destino      | fase   | decisao                                       | relevancia_v1   | observacoes                                                                                                                               |
|:-----------------------------------|------:|:-----------------|:------------------------------------------------------------------------------------------------------|:--------------------|:-------|:----------------------------------------------|:----------------|:------------------------------------------------------------------------------------------------------------------------------------------|
| amostras_importacao_dominio_txt_43 |     4 | gabarito_externo | Especificação real do layout de importação do Domínio (linhas de 43 posições).                        | serializer_v1       | V1     | usar como golden file primário                | SIM             | Arquivos fora do inventário do escritório; vêm do caso DELA MORE                                                                          |
| planilha_corrigida_marco_xlsx      |     1 | entrada_canonica | Modelo canônico de entrada para o motor.                                                              | ingestao_v1         | V1     | usar como contrato inicial de entrada         | SIM             | Base para schema e parser                                                                                                                 |
| resumo_folha_pagamento             |   198 | nucleo           | Reconciliação agregada por rubrica, empresa e competência.                                            | validacao           | V1     | obrigatório para reconciliação pós-geração    | SIM             | Melhor fonte para conferir totais por evento                                                                                              |
| folha_pagamento                    |   197 | nucleo           | Amostragem por colaborador e leitura de eventos efetivamente lançados.                                | validacao           | V1     | usar para auditoria por colaborador           | SIM             | Complementa o resumo                                                                                                                      |
| holerites_pagamento                |   204 | validacao        | Checagem do líquido e conferência amostral do resultado final.                                        | validacao           | V1     | usar em amostragem, não como motor principal  | PARCIAL         | Especialmente útil para incidentes                                                                                                        |
| dae                                |   175 | validacao        | Validação tributária pós-importação.                                                                  | auditoria           | V2     | guardar para trilha fiscal                    | NAO             | Não entra na geração do TXT                                                                                                               |
| gfd                                |   260 | validacao        | Conferência de bases e encargos.                                                                      | auditoria           | V2     | guardar para trilha fiscal                    | NAO             | Não entra na geração do TXT                                                                                                               |
| re_gfd                             |   180 | validacao        | Conferência complementar de bases.                                                                    | auditoria           | V2     | guardar para trilha fiscal                    | NAO             | Não entra na geração do TXT                                                                                                               |
| gfd_consignado                     |   160 | validacao        | Conferência de consignados e descontos.                                                               | auditoria           | V2     | guardar para trilha fiscal                    | NAO             | Entrará quando houver módulo de consignado                                                                                                |
| folha_adiantamento                 |    98 | nucleo           | Casos de adiantamento salarial.                                                                       | modulo_adiantamento | V2     | separar como fluxo próprio                    | NAO             | Não misturar com folha mensal na V1                                                                                                       |
| resumo_folha_adiantamento          |    98 | nucleo           | Reconciliação do módulo de adiantamento.                                                              | modulo_adiantamento | V2     | usar quando módulo adiantamento existir       | NAO             | Fluxo próprio                                                                                                                             |
| holerites_adiantamento             |   101 | validacao        | Validação amostral do módulo de adiantamento.                                                         | modulo_adiantamento | V2     | futuro                                        | NAO             | Fluxo próprio                                                                                                                             |
| liquido_pagamento                  |    27 | validacao        | Conferência de valor líquido e pagamento.                                                             | auditoria           | V2     | usar como trilha auxiliar                     | PARCIAL         | Útil quando disponível                                                                                                                    |
| liquido_adiantamento               |    14 | validacao        | Conferência de pagamento do adiantamento.                                                             | modulo_adiantamento | V2     | futuro                                        | NAO             | Fluxo próprio                                                                                                                             |
| folha_plr                          |    30 | nucleo           | Fluxo de PLR separado da folha mensal.                                                                | modulo_plr          | V3     | separar como módulo próprio                   | NAO             | Não entra na V1                                                                                                                           |
| resumo_folha_plr                   |     8 | nucleo           | Reconciliação PLR.                                                                                    | modulo_plr          | V3     | futuro                                        | NAO             | Fluxo próprio                                                                                                                             |
| holerites_plr                      |    20 | validacao        | Validação amostral PLR.                                                                               | modulo_plr          | V3     | futuro                                        | NAO             | Fluxo próprio                                                                                                                             |
| folha_ponto                        |    20 | nucleo           | Fonte upstream para normalizar horas e faltas antes da planilha canônica.                             | normalizador_ponto  | V3     | guardar para módulo anterior à planilha       | NAO             | Não mistura com serializer V1                                                                                                             |
| txt_btc_cnab_240                   |     5 | apoio            | TXT do acervo BTC são remessas CNAB bancárias de 240 posições, não arquivos de importação do Domínio. | modulo_cnab         | V3     | separar explicitamente do motor de importação | NAO             | Úteis para futuro módulo de pagamento bancário, não para V1                                                                               |
| outros_whitelist                   |   496 | apoio            | Whitelistar apenas Demonstrativo INSS, Encargos IRRF/INSS, Recibos de VT/VA/Cesta e Líquidos.         | apoio               | V2     | processar por whitelist de filename           | NAO             | Demais “outros” ficam em quarentena; whitelist inicial inclui Demonstrativo INSS, Encargos de IRRF/INSS, líquidos e recibos de benefícios |
| trct_ferias_rescisao               |   258 | secundario       | Conjunto para rescisão/férias.                                                                        | modulos_futuros     | V3     | não entrar no escopo do motor mensal          | NAO             | Módulos futuros                                                                                                                           |

### Whitelist inicial para arquivos classificados como “outros”

| prefix                     |   qtd |
|:---------------------------|------:|
| ENCARGOS DE IRRF           |    13 |
| RECIBO DE VALE TRANSPORTE  |     8 |
| LIQUIDO ADIANTAMENTO       |     7 |
| DEMONSTRATIVO INSS         |     6 |
| ENCARGOS DE INSS           |     6 |
| RECIBO DE VALE ALIMENTAÇÃO |     5 |
| RECIBO DE CESTA BÁSICA     |     4 |
| RECIBO AJUDA DE CUSTO      |     4 |
| LIQUIDO FOLHA DE PAGAMENTO |     2 |
| LÍQUIDOS DE ADIANTAMENTO   |     2 |

## Arquitetura alvo

```text
XLSX canônico
   -> Ingestão e limpeza
   -> Modelo canônico interno
   -> Mapeamento por empresa/filial
   -> Serializer TXT 43 posições
   -> Validação estrutural
   -> Reconciliação contra PDFs oficiais
   -> Bundle de saída (TXT + manifesto + relatório + pendências)
```

## Contratos mínimos de dados

### Contrato de entrada (planilha)

- Campos básicos: código, funcionário, eventos lançáveis, observações.
- Eventos inicialmente suportados para automação direta: **20, 150, 257, 258, 259, 260, 262, 238, 264, 8111**.
- Eventos com política explícita/pendência: **48, 265, faltas, atrasos, observações textuais**.

### Contrato de configuração por empresa

- Empresa/filial Domínio.
- Matrícula Domínio por colaborador.
- Mapeamento evento de negócio -> rubrica de saída.
- Tipo de processo/cálculo.
- Regras de revisão obrigatória.

### Contrato de saída

- Um TXT por empresa/processo.
- Manifesto com hash do input canônico, hash do output, versão do motor e pendências.
- Relatório de reconciliação.

## Ondas de implantação

| onda                 |   empresas |
|:---------------------|-----------:|
| cauda                |         75 |
| expansao             |         13 |
| piloto_generalizacao |          6 |
| piloto_negocio       |          5 |

### Prioridade operacional recomendada

1. **Piloto de negócio**: DELA MORE (porque já existe planilha corrigida + amostras reais de importação + resumos para conferência).
2. **Piloto de generalização**: FORTEBOX ou MAC (alta densidade de documentos para provar que o motor generaliza por configuração).
3. **Expansão**: empresas com boa cobertura de núcleo/validação no inventário.
4. **Cauda longa**: empresas com cobertura parcial entram só após estabilização.

### Empresas mais fortes para cada trilha

| company               | onda_implantacao     |   nucleo |   ouro |   validacao |   meses | racional                                                                                        |
|:----------------------|:---------------------|---------:|-------:|------------:|--------:|:------------------------------------------------------------------------------------------------|
| BTC                   | piloto_generalizacao |       14 |      5 |          29 |       4 | possui TXT no acervo, porém TXT é CNAB/remessa e serve para módulo futuro de pagamento bancário |
| MAC                   | piloto_generalizacao |       17 |      0 |          33 |       3 | alta densidade de arquivos núcleo e validação                                                   |
| FORTEBOX              | piloto_generalizacao |       18 |      0 |          30 |       4 | alta densidade de arquivos núcleo e validação                                                   |
| MAIS QUE BOLO         | piloto_generalizacao |       12 |      0 |          22 |       3 | alta densidade de arquivos núcleo e validação                                                   |
| SAAD TOSSI            | piloto_generalizacao |       12 |      0 |          22 |       3 | alta densidade de arquivos núcleo e validação                                                   |
| ZANATELI E SAKAI      | piloto_generalizacao |       12 |      0 |          21 |       3 | alta densidade de arquivos núcleo e validação                                                   |
| DELA MORE - MATRIZ    | piloto_negocio       |        4 |      0 |          23 |       4 | caso real já estudado; possui folha/resumo e pode consumir o gabarito de importação separado    |
| DELA MORE - ARAÇATUBA | piloto_negocio       |        4 |      0 |           3 |       3 | caso real já estudado; possui folha/resumo e pode consumir o gabarito de importação separado    |
| DELA MORE - AMERICANA | piloto_negocio       |        4 |      0 |           2 |       2 | caso real já estudado; possui folha/resumo e pode consumir o gabarito de importação separado    |
| DELA MORE - PARANA    | piloto_negocio       |        4 |      0 |           2 |       2 | caso real já estudado; possui folha/resumo e pode consumir o gabarito de importação separado    |
| DELA MORE - SJRP      | piloto_negocio       |        4 |      0 |           2 |       2 | caso real já estudado; possui folha/resumo e pode consumir o gabarito de importação separado    |

## Estrutura sugerida do repositório

```text
payroll-txt-engine/
  docs/
    specs/
    runbooks/
  configs/
    companies/
      dela_more_72.yaml
      dela_more_78.yaml
      fortebox.yaml
  data/
    golden/
      dominio_import/
      folha_pagamento/
      resumo_folha/
    fixtures/
  src/
    domain/
    ingestion/
    mapping/
    serializer/
    validators/
    review/
    cli/
  tests/
    unit/
    integration/
    golden/
  scripts/
```

## Guard-rails de segurança

- **Nada de IA gerando TXT em produção sem validação determinística**.
- **Nenhuma nova regra entra sem fixture + teste + reconciliação agregada**.
- **Mudança de mapeamento por empresa entra por configuração versionada, não por hardcode**.
- **Qualquer campo ambíguo bloqueia exportação ou cai em pendência explícita**.
- **Toda execução gera run_id, hashes e relatório**.

## Fluxo de trabalho com VS Code, Codex e ChatGPT

1. **VS Code** como ambiente principal do repositório, testes e depuração.
2. **Codex/ChatGPT** para gerar parser, testes, documentação e refatorações pequenas.
3. Toda sugestão de IA deve resultar em um **artefato verificável**: teste, fixture, spec ou diff de configuração.
4. O time humano continua responsável por: aprovar regra, homologar empresa, revisar pendências e liberar versão.

## Sequência prática de implantação

### Etapa 0 - Fundar a evidência

- Consolidar o dataset dourado do caso DELA MORE.
- Separar os PDFs do inventário em V1/V2/V3.
- Registrar formalmente que BTC/TXT = CNAB futuro, não Domínio import.

### Etapa 1 - Fazer o motor mínimo funcionar

- Ler a planilha corrigida.
- Gerar o TXT 43-posições.
- Passar nos golden tests.
- Reconciliação por rubrica contra Resumo Folha.

### Etapa 2 - Tornar seguro

- Manifesto e hashes.
- Relatórios de divergência.
- Fila de pendências.
- Runbook de homologação.

### Etapa 3 - Generalizar

- Subir FORTEBOX/MAC por configuração.
- Subir onda de expansão.
- Somente depois atacar planilha bagunçada, ponto, PLR e CNAB.

## Backlog técnico

O backlog detalhado foi exportado em CSV com **51 itens**.

| id          | epic            | wave   | priority   | title                                             |
|:------------|:----------------|:-------|:-----------|:--------------------------------------------------|
| SETUP-001   | Fundação        | V1     | P0         | Criar repositório e estrutura base                |
| SETUP-002   | Fundação        | V1     | P0         | Configurar ambiente de qualidade                  |
| SETUP-003   | Fundação        | V1     | P0         | Definir convenção de dados sensíveis              |
| SETUP-004   | Fundação        | V1     | P1         | Criar banco local de auditoria                    |
| SPEC-001    | Especificação   | V1     | P0         | Documentar layout TXT Domínio 43 posições         |
| SPEC-002    | Especificação   | V1     | P0         | Definir schema da planilha canônica               |
| SPEC-003    | Especificação   | V1     | P0         | Catalogar eventos base e tipo de quantidade       |
| SPEC-004    | Especificação   | V1     | P0         | Definir schema de configuração por empresa        |
| SPEC-005    | Especificação   | V1     | P1         | Definir taxonomia de erros e pendências           |
| DATA-001    | Dados           | V1     | P0         | Montar pacote de golden files                     |
| DATA-002    | Dados           | V1     | P0         | Separar ativos do inventário por módulo           |
| DATA-003    | Dados           | V2     | P1         | Whitelistar arquivos úteis dentro de “outros”     |
| ING-001     | Ingestão        | V1     | P0         | Ler XLSX preservando tipos                        |
| ING-002     | Ingestão        | V1     | P0         | Detectar e limpar linhas não lançáveis            |
| ING-003     | Ingestão        | V1     | P0         | Normalizar números e moeda brasileira             |
| ING-004     | Ingestão        | V1     | P0         | Normalizar horas                                  |
| ING-005     | Ingestão        | V1     | P1         | Produzir snapshot canônico da entrada             |
| ING-006     | Ingestão        | V2     | P1         | Criar normalizador da planilha bagunçada          |
| MAP-001     | Mapeamento      | V1     | P0         | Mapear colunas para eventos de negócio            |
| MAP-002     | Mapeamento      | V1     | P0         | Mapear evento de negócio para rubrica por empresa |
| MAP-003     | Mapeamento      | V1     | P0         | Criar tabela de matrícula Domínio                 |
| MAP-004     | Mapeamento      | V1     | P1         | Resolver tipo de processo/cálculo                 |
| MAP-005     | Mapeamento      | V1     | P1         | Implementar política de pendência obrigatória     |
| MAP-006     | Mapeamento      | V1     | P1         | Definir ordenação determinística de registros     |
| SER-001     | Serializer      | V1     | P0         | Implementar serializer fixed-width                |
| SER-002     | Serializer      | V1     | P0         | Implementar encoder de valor e encoder de horas   |
| SER-003     | Serializer      | V1     | P1         | Gerar nomes de arquivo e pacote de saída          |
| SER-004     | Serializer      | V1     | P1         | Assinar hash da execução                          |
| VAL-001     | Validação       | V1     | P0         | Validar linha a linha                             |
| VAL-002     | Validação       | V1     | P0         | Criar golden tests com amostras de importação     |
| VAL-003     | Validação       | V1     | P0         | Reconciliação agregada contra Resumo Folha        |
| VAL-004     | Validação       | V1     | P1         | Amostragem por colaborador contra Folha/Holerite  |
| VAL-005     | Validação       | V1     | P1         | Criar relatório de divergências e pendências      |
| VAL-006     | Validação       | V2     | P1         | Validar tributos com DAE/GFD/RE GFD               |
| UI-001      | Interface       | V1     | P0         | Criar CLI generate                                |
| UI-002      | Interface       | V1     | P1         | Criar CLI validate                                |
| UI-003      | Interface       | V1     | P1         | Criar preview tabular                             |
| UI-004      | Interface       | V2     | P2         | Criar interface web local                         |
| OPS-001     | Operação        | V1     | P0         | Definir fluxo de trabalho VS Code/Codex/ChatGPT   |
| OPS-002     | Operação        | V1     | P0         | Criar runbook de homologação                      |
| OPS-003     | Operação        | V1     | P1         | Criar checklist de release                        |
| AI-001      | IA e Segurança  | V1     | P0         | Impor regra: IA fora do core determinístico       |
| AI-002      | IA e Segurança  | V1     | P1         | Criar prompts padrão para Codex/ChatGPT           |
| AI-003      | IA e Segurança  | V2     | P1         | Assistente de classificação de observações        |
| MOD-001     | Módulos futuros | V2     | P1         | Implementar módulo de adiantamento                |
| MOD-002     | Módulos futuros | V3     | P2         | Implementar módulo PLR                            |
| MOD-003     | Módulos futuros | V3     | P2         | Implementar normalizador de ponto                 |
| MOD-004     | Módulos futuros | V3     | P2         | Implementar módulo CNAB bancário                  |
| ROLLOUT-001 | Rollout         | V1     | P0         | Pilotar com Dela More                             |
| ROLLOUT-002 | Rollout         | V1     | P1         | Generalizar com FORTEBOX ou MAC                   |
| ROLLOUT-003 | Rollout         | V2     | P1         | Onboarding em lote por ondas                      |

## Critério de pronto para produção

- Gera TXT estruturalmente válido.
- Reconcilia com o Resumo Folha sem divergência material não explicada.
- Possui manifesto, hashes e relatório.
- Toda regra crítica está coberta por teste.
- Operador consegue identificar claramente o que foi automatizado e o que ficou pendente.