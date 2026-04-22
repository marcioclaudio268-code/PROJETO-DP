# Schema da configuracao por empresa

## Proposito

A configuracao por empresa e versionada e substitui qualquer hardcode no codigo de dominio.

## Contrato inicial

| Campo | Obrigatorio | Tipo inicial | Descricao | Status |
| --- | --- | --- | --- | --- |
| codigo_empresa | Sim | string | Codigo interno da empresa ou filial alvo. | Fechado estruturalmente |
| nome | Sim | string | Nome legivel da empresa. | Fechado estruturalmente |
| processo_padrao | Sim | string | Processo padrao da empresa no motor. O formato final continua ABERTO. | ABERTO |
| competencia | Sim | string | Competencia atendida por esta configuracao. | Fechado estruturalmente |
| config_version | Sim | string | Versao da configuracao. | Fechado estruturalmente |
| event_mappings | Sim | lista[EventMapping] | Mapeamento `evento_negocio -> rubrica_saida`. | Fechado estruturalmente |
| employee_mappings | Sim | lista[EmployeeMapping] | Resolucao de matricula por chave de origem. | Fechado estruturalmente |
| pending_policy | Sim | PendingPolicy | Politicas de pendencia obrigatoria. | Fechado estruturalmente |
| validation_flags | Nao | mapa[str,bool] | Flags extensiveis de validacao. A lista de flags nao deve ser hardcoded por empresa no dominio. | ABERTO |
| notes | Nao | string | Observacoes gerais da configuracao. | ABERTO |

## Modelos relacionados

- `EventMapping`
- `EmployeeMapping`
- `PendingPolicy`
- `RunManifest`

## Regras minimas

- O dominio nao pode conter logica especifica de empresa.
- Toda diferenca de comportamento deve entrar por configuracao versionada.
- Mudancas de mapeamento exigem nova versao de configuracao.
- Flags de validacao devem permanecer extensiveis.

## Uso operacional atual

- A CLI atual de mapping consome `CompanyConfig` serializado em JSON UTF-8.
- `event_mappings` duplicados sao rejeitados no carregamento da configuracao.
- `employee_mappings` duplicados por `source_employee_key` sao rejeitados no carregamento da configuracao.
- Ambiguidade introduzida por `aliases` continua sendo tratada na etapa de mapping como pendencia explicita, nunca como decisao silenciosa.

## Pendencias abertas

- Forma final de serializacao da configuracao em arquivo.
- Estrategia de versionamento por competencia versus versao logica.
- Regras de prioridade quando houver mais de uma chave de resolucao.
- Regras de vigencia temporal para employee mappings.
