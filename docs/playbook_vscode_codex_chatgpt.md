# Playbook de desenvolvimento com VS Code, Codex e ChatGPT

## Regra de ouro

Use IA para acelerar a construção do software; não para substituir o motor determinístico de folha.

## Ciclo de trabalho recomendado

1. Abrir uma issue do backlog.
2. Escrever/atualizar a spec antes do código quando a regra mudar.
3. Pedir ao Codex/ChatGPT uma implementação **junto com testes**.
4. Rodar testes locais e golden tests.
5. Revisar diff de código e diff de config.
6. Registrar amostras novas em /data/golden quando a regra for homologada.

## Tipos de prompt úteis

- “Implemente o parser da planilha canônica e já escreva testes para horas, moeda BR e linhas vazias.”
- “Crie função pura para serializar linha 43-posições e valide comprimentos/zero-padding.”
- “Refatore o mapeamento por empresa para schema YAML validado por Pydantic e preserve os testes.”
- “Escreva golden tests comparando a saída gerada com os TXT do dataset Dela More.”

## O que nunca delegar cegamente à IA

- Aprovação de regra de negócio.
- Alteração de rubrica por empresa sem evidência.
- Interpretação silenciosa de observações, faltas e atrasos.
- Liberação de versão sem reconciliação.
