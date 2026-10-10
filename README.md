# Laboratório BD · Do dado à proteção

Pipeline de dados de uma loja online, da ingestão à proteção, construído em cinco entregas semanais
(tags `entrega-1` a `entrega-5`).

## Grupo

- _Nome completo · RA_
- _Nome completo · RA_
- _Nome completo · RA_

## Escolhas do grupo

| Item | Escolha | Motivo |
|---|---|---|
| Plataforma | Databricks Free Edition (gratuito, serverless) | Ambiente no navegador, sem cartão, com Unity Catalog e Delta Lake |
| Linguagem | Python (PySpark) e SQL | Linguagens suportadas na Free Edition |
| Banco da origem | Tabelas Delta no schema `workspace.lab_origem` | Simula o banco transacional da loja |
| Camadas de destino | Tabelas Delta em `lab_bronze`, `lab_silver` e `lab_gold` | Mesmo catálogo, schemas separados por camada |
| Estado do pipeline | Schema `lab_controle` (marcas-d'água, offsets) | Estado fora do código |
| Desenho | _a definir: diagrams.net, Excalidraw ou Mermaid_ | |
| Trilha de captura de mudanças | _a definir na Missão 2_ | |

## Como rodar do zero

1. No Databricks, abrir este repositório como Git folder.
2. Rodar `ingestao/00_criar_origem` (cria os schemas e carrega o dia 1 na origem).

_Os próximos passos entram a cada missão._

## Estrutura

```
dados/          dia 1, mudanças do dia 2, lote do dia 3, sintético
contratos/      pedidos.yaml, clientes.yaml, navegacao.yaml
ingestao/       carga incremental, log, produtor e leitores
qualidade/      validação e quarentena
governanca/     acordo.yaml, catalogo.md, regras como código
seguranca/      matriz.md, pseudonimização, camada silver
privacidade/    visão de consumo, moradas.md, esquecer
resiliencia/    plano.md, cópia de segurança, reconstruir
arquitetura/    fonte e imagem do desenho
evidencias/     números de cada missão
```

Os dados são fictícios: os CPFs são inválidos de propósito e os IPs pertencem à faixa reservada para documentação (203.0.113.0/24).
