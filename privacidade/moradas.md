# Moradas da cliente 5

A cliente 5 pediu a eliminação dos dados (dia 2). A exclusão no cadastro foi só o começo: o dado dela tinha sido
copiado para muitos lugares. Este inventário lista **cada morada**, a **técnica** usada pelo comando `esquecer`
(`privacidade/08a_esquecer`) e **o porquê**. Os valores da cliente não aparecem aqui de propósito.

| # | Morada | O que havia dela | Técnica | Por quê |
|---|---|---|---|---|
| 1 | `lab_origem.clientes` | cadastro (nome, e-mail, CPF, nascimento, cidade) | **apagar** a linha (já feito no dia 2; o comando confere) | é o pedido da cliente |
| 2 | `lab_origem.pedidos` · pedido 106 | `cliente_id` e telefone na `observacao` | **anonimizar**: `cliente_id` e `observacao` vazios | o pedido é registro de venda com guarda fiscal; fica, mas sem a pessoa |
| 3 | `lab_origem.navegacao` | 2 cliques com o IP dela | **apagar** os eventos | clique sem a pessoa não tem uso, e o IP identifica |
| 4 | `lab_log.mudancas` · evento de exclusão | a linha inteira dela, como era antes (imagem "antes") | **apagar os dados** do evento e manter seq, operação e chave | o evento precisa existir para reaplicar a exclusão sobre uma cópia antiga (Missão 9); os dados não |
| 5 | `lab_log.mudancas` · eventos de pedidos | `cliente_id` e observação no JSON (se houver) | **anonimizar** o JSON | mesmo motivo do pedido |
| 6 | `lab_log.navegacao` (tópico) | os 2 eventos, com IP | **apagar** os eventos | o tópico guarda por tempo, não até ser lido; não pode ficar com ela |
| 7 | `lab_leitores.visitas_eventos` e `por_cliente_eventos` | registro dos eventos dela já contados (estado dos leitores) | **apagar** pelos `evento_id` | as contagens (`visitas_por_pagina`, `eventos_por_cliente`) são recalculadas sem ela |
| 8 | `lab_controle.offsets` e `leituras` | só posições e contagens | **nada** | não guardam dado pessoal; os offsets continuam valendo |
| 9 | `lab_bronze.clientes` | cadastro | **apagar** (o log já tinha aplicado a exclusão; o comando confere) | cópia fiel da origem |
| 10 | `lab_bronze.pedidos` · 106 | `cliente_id`, telefone na observação | **anonimizar** | igual à origem |
| 11 | `lab_bronze.pedidos_lote` | nada dela no lote do dia 3 | anonimizar se houver | o comando trata qualquer cliente |
| 12 | `lab_bronze.navegacao` | os 2 eventos, com IP | **apagar** | igual ao tópico |
| 13 | `lab_qualidade.quarentena` | só como pseudônimo, se alguma linha dela fosse reprovada | **apagar o pseudônimo** da linha | a quarentena guarda o motivo, não a pessoa |
| 14 | `lab_qualidade.varredura` | só onde o dado estava (chaves), nunca o valor | **refazer** o varredor depois | a saída não guarda dado pessoal |
| 15 | `lab_silver.clientes`, `pedidos`, `navegacao` | `cliente_ps` (pseudônimo dela) no pedido 106 e nos 2 cliques | **apagar** os cliques, **esvaziar** o `cliente_ps` do 106 | pseudônimo continua sendo dado pessoal enquanto houver chave e bronze para voltar à pessoa |
| 16 | `lab_gold.receita_por_cidade` | nada por pessoa (visão agregada) | **nada**: é recalculada; o 106 continua na linha de outras | o valor da venda fica na receita |
| 17 | Histórico do Delta (versões antigas e Change Data Feed de todas as tabelas acima) | o dado como era antes de cada mudança | **VACUUM**: imediato quando a plataforma permite `RETAIN 0 HOURS`; senão, em até 7 dias | time travel devolveria a cliente; no serverless o prazo mínimo é o de retenção do Delta |
| 18 | Tabelas apagadas da silver (Missão 6) | observação com o telefone, nas versões sem proteção | some com o prazo de `UNDROP` do Unity Catalog (7 dias) | o Unity Catalog guarda tabela apagada por 7 dias |
| 19 | Saídas de notebooks já rodados | o que foi mostrado na tela (bronze, log) | **Clear all outputs** nos notebooks de ingestão | a saída fica gravada no notebook |
| 20 | `dados/dia1/*.csv` no repositório | a entrada fictícia do laboratório | **exceção registrada**: fica, e a lista de eliminação é reaplicada toda vez que essa entrada ou uma cópia antiga for carregada | é o "backup" do dia 1 que a Missão 9 restaura |
| 21 | Cópias de segurança (Missão 9) | a bronze no estado do dia 1 | **reaplicar a lista** depois de toda restauração | cópia não se edita; a lista garante que a cliente não volta |
| 22 | Chave do pseudônimo (secret scope) | nada dela | **nada** | sem as linhas, o pseudônimo dela não aponta para nada |

## Lista de eliminação

`lab_controle.eliminacoes` guarda **um registro por pedido de eliminação**, sem dado pessoal:

- `chave_ps`: o pseudônimo HMAC do cliente. Não é o id nem o nome; sem a chave e sem a bronze, não volta à pessoa.
  É por ele que a lista é reaplicada sobre uma cópia restaurada: o comando calcula o pseudônimo de cada id da
  cópia e trata as linhas que batem.
- `pedido_em` e `executado_em`.
- `moradas`: quantas linhas foram tratadas em cada morada na primeira execução.

`lab_controle.anonimizados` registra os pedidos que ficaram sem pessoa (só o número do pedido). O contrato de
`pedidos` marca `cliente_id` como `anonimizavel`: obrigatório, mas aceito vazio nessas linhas. Assim o pedido 106 não
cai na quarentena e o valor dele continua na receita.
