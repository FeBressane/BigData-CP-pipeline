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
3. Missão 1, nesta ordem:
   1. `ingestao/01_carga_incremental` (1ª carga: 6 clientes, 8 pedidos, 8 eventos)
   2. `ingestao/01_carga_incremental` de novo (lê 0 linhas)
   3. `ingestao/01b_aplicar_dia2` (aplica as 5 mudanças do dia 2 na origem)
   4. `ingestao/01_carga_incremental` (lê 4 linhas)
   5. `ingestao/01c_verificar_missao1` (confere e grava `evidencias/missao1.txt`)

   Para refazer do zero: rodar `00_criar_origem` e depois `01_carga_incremental` com o widget
   "Apagar bronze e estado antes?" em `sim`.

_Os próximos passos entram a cada missão._

## Decisões técnicas

- **Marca-d'água fora do código.** Fica em `lab_controle.marca_dagua`, uma linha por tabela.
  `clientes` e `pedidos` usam o maior `atualizado_em` carregado; `navegacao` usa o maior `evento_id`,
  porque o evento não muda depois de criado e o `quando` chega fora de ordem (o evento 8 é anterior ao 6).
- **Mesclagem pela chave.** `MERGE` do Delta: linha nova entra, linha existente é substituída.
  Rodar de novo com a mesma entrada não muda o resultado.
- **Anexação sem duplicata.** A navegação entra por `MERGE ... WHEN NOT MATCHED THEN INSERT`, que só
  acrescenta e usa o `evento_id` para ignorar o que já está na bronze.
- **Limite conhecido.** A marca-d'água compara `atualizado_em > marca`. Uma linha gravada na origem com
  horário igual ou anterior ao da marca, depois da carga, não seria lida. E um `DELETE` não deixa linha
  para ler: a cliente 5 continua na bronze. A Missão 2 resolve com o log de mudanças.

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
