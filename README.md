# Laboratório BD · Do dado à proteção

Pipeline de dados de uma loja online, da ingestão à proteção, construído em cinco entregas semanais
(tags `entrega-1` a `entrega-5`).

## Equipe · Turma 4ESPV

| Integrante | RM |
|---|---|
| Camila do Prado Padalino | 98316 |
| Gabriel Teixeira Machado | 551570 |
| Felipe Cavalcante Bressane | 97688 |

## Escolhas do grupo

| Item | Escolha | Motivo |
|---|---|---|
| Plataforma | Databricks Free Edition (gratuito, serverless) | Ambiente no navegador, sem cartão, com Unity Catalog e Delta Lake |
| Linguagem | Python (PySpark) e SQL | Linguagens suportadas na Free Edition |
| Banco da origem | Tabelas Delta no schema `workspace.lab_origem` | Simula o banco transacional da loja |
| Camadas de destino | Tabelas Delta em `lab_bronze`, `lab_silver` e `lab_gold` | Mesmo catálogo, schemas separados por camada |
| Estado do pipeline | Schema `lab_controle` (marcas-d'água, offsets) | Estado fora do código |
| Desenho | _a definir: diagrams.net, Excalidraw ou Mermaid_ | |
| Trilha de captura de mudanças | **Trilha B**: Change Data Feed do Delta nas tabelas da origem | O próprio banco registra cada mudança na ordem do commit; nenhuma mudança escapa, mesmo feita por fora do aplicativo |
| Log de mudanças | Tabela `lab_log.mudancas` (só anexa) | Sequência, tabela, operação, chave e dados de cada evento |
| Tópico de navegação | Tabela `lab_log.navegacao` (só anexa), Trilha A sem broker | O Kafka não é alcançável a partir da Free Edition; uma tabela só-anexa com offsets por leitor tem o mesmo comportamento |

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

4. Missão 2, nesta ordem:
   1. `ingestao/00_criar_origem` (origem de volta ao dia 1)
   2. `ingestao/02a_preparar_captura` (liga o CDF, bronze no dia 1, log vazio)
   3. `ingestao/01b_aplicar_dia2` (as 5 mudanças do dia 2)
   4. `ingestao/02_sincronizar` (captura 5 eventos e aplica na bronze)
   5. `ingestao/02_sincronizar` com "Reaplicar desde o início?" = `sim` (mesmos números)
   6. `ingestao/02c_verificar_missao2` (confere e grava `evidencias/missao2.txt`)

   A partir da Missão 2, `clientes` e `pedidos` chegam à bronze pelo log. A carga por marca-d'água
   (`01_carga_incremental`) fica como registro da Missão 1 e segue valendo para a `navegacao` até a Missão 3.

5. Missão 3 (os leitores rodam ao mesmo tempo, cada um numa aba, todos na computação Serverless):
   1. `ingestao/03a_preparar_topico` (tópico vazio, offsets apagados, bronze.navegacao vazia)
   2. Abrir `ingestao/03c_leitor_visitas`, pôr "Cair no evento" = `4` e rodar. Ele fica esperando eventos
      (até 10 minutos; o produtor precisa ser disparado dentro desse prazo).
   3. Em outra aba, abrir `ingestao/03d_leitor_bronze` e rodar. Também fica esperando.
   4. Em outra aba, rodar `ingestao/03b_produtor`: publica os 8 eventos, um por segundo.
      O `visitas` cai depois do 4º evento (erro proposital); a bronze segue gravando em rajadas.
   5. Quando o produtor terminar, voltar ao `03c_leitor_visitas`, pôr "Cair no evento" = `0` e rodar de novo.
   6. `ingestao/03e_leitor_por_cliente` (liga depois de tudo publicado e lê desde o offset 0)
   7. `ingestao/03f_verificar_missao3` (confere e grava `evidencias/missao3.txt`)

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
- **Trilha B (CDF).** `02a_preparar_captura` liga `delta.enableChangeDataFeed` em `clientes` e `pedidos`
  e guarda a versão Delta atual como offset da captura. `02_sincronizar` lê `table_changes()` a partir
  da versão seguinte, descarta as imagens "antes" das alterações (`update_preimage`) e anexa ao log
  na ordem do commit. A Trilha A (tabela `mudancas` preenchida pelo programa) foi descartada porque
  qualquer mudança feita por fora do programa escaparia do log.
- **Dois offsets, dois leitores.** A captura guarda a última versão lida de cada tabela; a bronze guarda
  o último `seq` aplicado. Cair e voltar é retomar dali.
- **Aplicar em ordem, de forma idempotente.** Para cada chave vale o último evento do trecho lido; o
  `MERGE` apaga (exclusão), substitui (alteração) ou insere (inserção). Reaplicar o log inteiro deixa a
  bronze igual.
- **Exclusão chega ao destino.** O evento de exclusão da cliente 5 apaga a linha da bronze. O pedido 106
  continua, sem chave estrangeira. O mesmo caminho atende ao pedido de eliminação da Missão 8.
- **Tópico de navegação (Missão 3, Trilha A).** `lab_log.navegacao` é uma tabela Delta em que só se anexa,
  com `seq` por evento e chave igual ao `cliente_id`. O produtor não conhece os leitores. Cada leitor
  (`visitas`, `bronze`, `por_cliente`) guarda o próprio offset em `lab_controle.offsets` e registra cada
  rodada em `lab_controle.leituras`. A carga em lote da navegação (Missão 1) saiu de cena.
- **Quando salvar o offset: depois de processar.**
  - *Salvando depois* (a nossa escolha): se o leitor cair entre processar e salvar, ao voltar ele lê de
    novo o último trecho. Nada se perde, mas um evento pode chegar duas vezes (**pelo menos uma vez**).
  - *Salvando antes*: se cair entre salvar e processar, ao voltar ele pula o trecho que não terminou.
    Nada se repete, mas eventos podem se perder (**no máximo uma vez**).
  - Escolhemos não perder eventos e tornar o processamento idempotente: cada leitor registra o `evento_id`
    que já contou e ignora repetidos. O teste derruba o `visitas` depois do 4º evento, antes de salvar o
    offset; ao religar, ele relê o 4º evento, reconhece que já foi contado e chega aos mesmos totais.
- **Latência no Databricks serverless.** Cada escrita numa tabela Delta leva cerca de 1 a 2 segundos, então
  "um evento por segundo" e "leitor na hora" ficam um pouco mais lentos que num broker de verdade. A
  diferença de ritmo entre o leitor rápido (acorda a cada 0,5 s) e a bronze (a cada 5 s) continua visível.
- **Moradas do log.** O log guarda a linha excluída (o evento de exclusão leva a imagem "antes") e o
  Delta guarda versões antigas da origem e da bronze. Os dois entram no inventário de moradas da Missão 8.

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
