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
| Desenho | Excalidraw (`arquitetura/estrutura.excalidraw` e `Estrutura.png`) | Desenho colaborativo no navegador, com fonte versionada |
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

5. Missão 3:
   1. `ingestao/03_simular` (um Run all faz tudo: prepara o tópico, publica os 8 eventos com os leitores
      acordando no seu ritmo, derruba o `visitas` no 4º evento, religa, e liga o leitor novo `por_cliente`)
   2. `ingestao/03f_verificar_missao3` (confere e grava `evidencias/missao3.txt`)

   Alternativa em tempo real, com os leitores em abas separadas: `03a_preparar_topico`, depois
   `03c_leitor_visitas` (com "Cair no evento" = 4) e `03d_leitor_bronze` rodando ao mesmo tempo, então
   `03b_produtor`; ao fim, religar o `03c` com 0 e rodar `03e_leitor_por_cliente`. Os leitores esperam até
   10 minutos pelo primeiro evento.

6. Missão 4:
   1. `ingestao/04a_carregar_lote_dia3` (o lote do dia 3 entra na bronze como chegou)
   2. `qualidade/04b_validar` (lê `contratos/*.yaml` e separa silver e quarentena)
   3. `qualidade/04c_verificar_missao4` (confere e grava `evidencias/missao4.txt`)

7. Missão 5:
   1. `governanca/05a_governanca` (regra como código: confere o acordo; para se faltar retenção, classe ou dono)
   2. `governanca/05b_gerar_catalogo` (gera `governanca/catalogo.md` a partir dos contratos e do acordo)
   3. `governanca/05c_varredor` (procura e-mail, telefone, CPF e IP na bronze e no log; grava `lab_qualidade.varredura`)
   4. `qualidade/04b_validar` (agora começa pela regra como código)
   5. `governanca/05d_verificar_missao5` (confere e grava `evidencias/missao5.txt`)

   Para ver o pipeline parar de verdade: apaguem a linha `prazo: 90 dias` da `navegacao` em
   `governanca/acordo.yaml`, rodem `qualidade/04b_validar` (para na primeira célula, com a mensagem do que falta),
   devolvam a linha e rodem de novo.

8. Missão 6:
   1. `seguranca/06b_criar_chave` (uma vez: cria a `CHAVE_PSEUDONIMO` no secret scope `lab_bd`, fora do repositório)
   2. `qualidade/04b_validar` (refaz a silver protegida: `cliente_ps` no lugar do id, sem nome, e-mail, CPF,
      observação nem IP; quarentena mascarada)
   3. `governanca/05b_gerar_catalogo` (o catálogo ganha a coluna "Na silver")
   4. `seguranca/06c_papeis` (grupos `lab_pipeline` e `lab_analistas`, permissões como código)
   5. `seguranca/06d_verificar_missao6` (confere e grava `evidencias/missao6.txt`, com o trajeto do CPF)

9. Missão 7:
   1. `qualidade/04b_validar` (a silver troca o nascimento pela faixa etária)
   2. `governanca/05b_gerar_catalogo` (o catálogo mostra a finalidade de cada campo pessoal)
   3. `privacidade/07a_visao_consumo` (mede o K e cria `lab_gold.receita_por_cidade` com K = 2)
   4. `privacidade/07b_verificar_missao7` (confere e grava `evidencias/missao7.txt`)

10. Missão 8:
    1. `privacidade/08a_esquecer` com o widget "Cliente a esquecer" = `5` (trata todas as moradas e grava a lista)
    2. `qualidade/04b_validar` (refaz a silver da bronze: os números têm que bater)
    3. `governanca/05c_varredor` (refaz a saída do varredor sem os eventos dela)
    4. `privacidade/08b_verificar_missao8` (confere, roda o esquecer de novo e grava `evidencias/missao8.txt`)

11. Missão 9:
    1. `resiliencia/09a_copia_dia1` (cópia da bronze do dia 1, offsets e cópia da chave)
    2. `resiliencia/09c_ensaio_desastre` com o widget "Confirmo apagar a bronze" = `sim` (apaga, restaura, reaplica
       log, tópico e lista, carrega o dia 3, reconstrói e cronometra)
    3. `resiliencia/09d_verificar_missao9` (confere e grava `evidencias/missao9.txt`)

    `resiliencia/09b_reconstruir` refaz só silver e gold a partir da bronze, a qualquer momento.

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
- **Latência no Databricks serverless e relógio lógico.** Cada escrita numa tabela Delta leva de 1 a 3
  segundos, então "um evento por segundo" fica mais lento que num broker de verdade, e coordenar três abas
  à mão no tempo certo se mostrou frágil (os leitores encerravam antes de o produtor publicar). O
  `03_simular` intercala produtor e leitores num só notebook e conta o tempo em ticks: 1 tick = 1 evento
  publicado = 1 segundo do laboratório. O `visitas` acorda a cada tick e a bronze a cada 5 ticks, a mesma
  proporção do exercício.
- **Contrato e quarentena (Missão 4).** Os contratos ficam em `contratos/*.yaml`. O validador
  (`qualidade/04b_validar`) lê o arquivo e transforma cada regra (`tipo`, `obrigatorio`, `unico`, `minimo`,
  `maximo`, `dominio`) numa expressão SQL; nenhuma regra fica fixa no código. Linha aprovada vai para
  `lab_silver.<conjunto>`; reprovada vai para `lab_qualidade.quarentena` com todos os motivos e a execução em
  que chegou. A silver é recalculada a partir da bronze a cada execução, e a quarentena é mesclada por
  origem + linha: rodar de novo não duplica nada.
- **O lote por arquivo fica na bronze como chegou.** `lab_bronze.pedidos_lote` guarda as linhas do CSV com
  todas as colunas como texto, mais o nome do arquivo e o número da linha. Assim a bronze não corrige nem
  descarta nada, e um valor que nem é número também pode ser barrado pela regra de tipo.
- **Chave repetida:** a primeira ocorrência passa; as seguintes vão para a quarentena.
- **Silver provisória.** Nesta missão a silver guarda as colunas do contrato como estão. Na Missão 6 ela é
  refeita sem nome, e-mail, CPF, observação nem IP, com o pseudônimo no lugar do id do cliente.
- **Acordo de governança (Missão 5).** `governanca/acordo.yaml` traz, para cada conjunto, as quatro decisões:
  dono (sempre um time), classe, retenção (prazo e motivo) e acesso (quem pede, quem aprova, auditoria). Os
  contratos ganharam a classe de cada campo e a descrição; a classe do conjunto é a do campo mais sensível, e os
  três conjuntos são `pessoal`.
- **O dono mais difícil: `pedidos`.** Três times mexem no pedido: Vendas cria e muda o status, Financeiro usa o
  valor para fechar o caixa e cumprir a obrigação fiscal, e Atendimento escreve a `observacao`. Ficou com o
  **time de Vendas** porque é quem gera o dado e decide o que cada status quer dizer: quando aparece um pedido
  `enviado` (Missão 4), é Vendas quem diz se o domínio muda ou se a origem está errada. Financeiro é o maior
  consumidor, não o dono: pede acesso e Vendas aprova. A `observacao` continua sob Vendas, mas é o campo que o
  Atendimento preenche, por isso o varredor olha para ela a cada execução.
- **Três prazos de retenção.**
  - *Navegação, 90 dias:* o clique serve para análise de visitas recentes e tem IP. Volume alto, valor que cai
    rápido: depois de 90 dias ficam só os agregados por página, sem IP e sem cliente.
  - *Pedidos, 5 anos após o fim do ano do pedido:* obrigação fiscal. O pedido é registro de venda e precisa ser
    guardado pelo prazo de guarda fiscal (a confirmar com o jurídico da loja). Se o cliente pedir eliminação, o
    pedido fica, mas sem a pessoa: o vínculo e a observação saem.
  - *Clientes, enquanto a conta estiver ativa:* o cadastro só tem uso enquanto há relação com o cliente. Conta
    encerrada ou pedido de eliminação (LGPD, art. 18): os dados pessoais saem de todas as camadas (Missão 8).
- **Regra como código.** `governanca/05a_governanca` lê o acordo e os contratos e lança erro, com a lista do que
  falta, quando um conjunto pessoal não tem prazo de retenção, um campo não tem classe, a classe do conjunto não é
  a do campo mais sensível ou o dono não é um time. O validador roda esse notebook na primeira célula: o pipeline
  não chega à silver com o acordo quebrado.
- **Catálogo gerado, nunca editado.** `governanca/catalogo.md` sai de `05b_gerar_catalogo`: descrição, campos e
  classes vêm do contrato; dono, retenção e acesso vêm do acordo; linhas e última alteração vêm da plataforma.
  Mudou o contrato, gera de novo.
- **Varredor de dado pessoal.** O produtor não tinha declarado a `observacao` dos pedidos (tem e-mail no 104 e
  telefone no 106) nem o `ip` da navegação. `05c_varredor` procura e-mail, telefone, CPF e IP em todas as colunas
  de texto da bronze e do log e compara com a classe do contrato; os dois achados viraram `classe: pessoal` com
  `classe_encontrada_pelo_varredor: true`. A saída guarda onde está o dado, nunca o valor.
- **Silver protegida (Missão 6).** Cada campo pessoal do contrato declara a sua proteção (`protecao`):
  `pseudonimo`, `remover` ou `manter`. O validador monta a silver a partir disso: o id do cliente vira `cliente_ps`
  nas três tabelas, e nome, e-mail, CPF, `observacao` e `ip` não entram. `nascimento` e `cidade` ficam por enquanto,
  como quase-identificadores; a Missão 7 registra a finalidade e generaliza. Campo pessoal sem proteção declarada
  para o pipeline (regra como código). A proteção exigida para cada classe está em `seguranca/matriz.md`.
- **Pseudônimo com HMAC.** `cliente_ps = HMAC-SHA-256(CHAVE_PSEUDONIMO, id)`. Um hash simples do id seria fácil de
  desfazer (basta calcular o hash de 1, 2, 3…); com a chave, não. A mesma chave dá sempre o mesmo pseudônimo, por
  isso pedidos, navegação e clientes continuam se ligando. A chave é criada por `seguranca/06b_criar_chave` e fica no
  secret scope `lab_bd` do Databricks: não está no código, no repositório, nos logs nem nas evidências, e a
  verificação procura o valor dela em todos os arquivos do repositório.
- **Versão antiga da silver.** A silver das Missões 4 e 5 tinha nome, e-mail e CPF, e o Delta guarda versões
  antigas. Na primeira execução protegida, o validador apaga a tabela e cria de novo, para que o time travel não
  devolva a versão sem proteção. A verificação lê todas as versões da silver.
- **Quarentena mascarada.** A linha reprovada continua na quarentena, mas os campos pessoais aparecem mascarados ou
  como pseudônimo: quem corrige precisa saber o motivo e a linha, não o dado.
- **Papéis: como a separação é garantida.** As permissões são código (`seguranca/06c_papeis`), aplicadas no Unity
  Catalog. O analista recebe só `USE SCHEMA` e `SELECT` em `lab_silver` e `lab_gold`, e leva `REVOKE` em origem, log,
  bronze, controle, qualidade e leitores; o pipeline é quem lê a bronze. O plano era usar dois grupos,
  `lab_pipeline` e `lab_analistas`, mas o Unity Catalog só aceita grupos da conta, e na Free Edition o grupo criado
  pelo workspace é local (`PRINCIPAL_DOES_NOT_EXIST`). Por isso o notebook cai no plano B: o analista é
  `account users`, ou seja, todo usuário da conta que não seja o dono só lê silver e gold; o pipeline é o dono dos
  schemas, que roda os notebooks e os jobs. O notebook mostra as permissões que ficaram valendo, inclusive as herdadas
  do catálogo, e para se o analista conseguir ler alguma camada com dado em claro. Numa conta com console de conta,
  os mesmos comandos rodam com os dois grupos (plano A), sem mudar o código.
- **Trajeto do CPF.** Real na origem, no log e na bronze; mascarado na quarentena; ausente na silver, na gold, no
  varredor, no catálogo e nas evidências (`seguranca/trajeto_cpf.md`). A verificação mede cada etapa. A evidência
  da Missão 2 mostrava o e-mail novo da cliente 3; passou a mostrar só que ele foi atualizado.
- **Finalidade (Missão 7).** Todo campo pessoal que fica na silver tem `finalidade` no contrato; sem finalidade,
  o campo sai, e a regra como código para o pipeline se faltar. A tabela está em `privacidade/finalidades.md`.
- **Pseudônimo não é anônimo.** Cidade e faixa etária são quase-identificadores. A data de nascimento virou faixa
  etária (a data exata não tinha finalidade) e o K da tabela de clientes é medido a cada execução
  (`lab_controle.medicoes_k`): com cidade + faixa, 4 combinações têm uma pessoa só (K = 1).
- **Visão de consumo com limiar K.** `lab_gold.receita_por_cidade` mostra cidade, número de clientes e receita paga.
  Cidade com menos de K = 2 clientes não aparece pelo nome. A receita dela vai para uma linha só, junto com os pedidos
  sem cliente, para que ninguém a recalcule subtraindo as outras cidades do total; a soma continua R$ 844,90.
- **Esquecer de verdade (Missão 8).** `privacidade/moradas.md` lista as 22 moradas da cliente 5 e a técnica de cada
  uma. O comando `esquecer <id>` apaga cadastro, cliques e eventos (origem, tópico, leitores, bronze, silver),
  apaga os dados do evento de exclusão no log (o evento fica, para reaplicar a exclusão numa cópia antiga) e deixa o
  pedido 106 sem pessoa: `cliente_id`, observação e `cliente_ps` vazios, valor na receita. O contrato marca
  `cliente_id` como `anonimizavel`, então o 106 não vai para a quarentena. Rodar de novo encontra 0 linhas.
- **Lista de eliminação sem dado pessoal.** A chave é o pseudônimo HMAC do cliente, nunca o id ou o nome. Depois de
  restaurar uma cópia, o comando reaplica a lista calculando o pseudônimo de cada id da cópia.
- **Histórico do Delta.** Versões antigas e o Change Data Feed também guardam o dado. O comando roda `VACUUM`; no
  serverless não dá para encurtar o prazo de retenção, então as versões antigas somem em até 7 dias.
- **Resiliência (Missão 9).** RPO e RTO de cada peça e o esquema 3-2-1-1-0 estão em `resiliencia/plano.md`. A cópia
  da bronze do dia 1 fica em tabelas Delta `appendOnly` e em Parquet num volume, com o offset do log e do tópico
  guardado junto. Silver e gold não têm cópia: o comando `reconstruir` recria as duas da bronze.
- **Ensaio do desastre.** Apagar a bronze, restaurar a cópia, reaplicar log e tópico, reaplicar a lista de eliminação,
  carregar o dia 3 e reconstruir, cronometrando cada etapa (`lab_controle.ensaios`). A lista vem depois do log porque
  a cópia do dia 1 ainda tem a cliente 5. A reaplicação usa a chave do evento de exclusão, já que o esquecer apagou os
  dados dele.
- **RTO real × prometido.** O ensaio levou 17,2 min contra 15 min prometidos: 72 s para restaurar a bronze e 16 min
  para reconstruir, quase todos na validação rodada num contexto novo (`dbutils.notebook.run`, workspace sob carga).
  A recuperação deu os mesmos números; o prazo foi revisto para 20 min e a correção está em `resiliencia/plano.md`.
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
