# Plano de resiliência

**RPO** (Recovery Point Objective): quanto dado podemos perder, medido em tempo. **RTO** (Recovery Time Objective):
quanto tempo o pipeline pode ficar fora até voltar. Os números abaixo valem para o volume deste laboratório
(dezenas de linhas, Databricks Free Edition serverless) e são defendidos linha a linha. O RTO da bronze até a gold é
medido de verdade no ensaio do desastre (`resiliencia/09c_ensaio_desastre`, resultado em `evidencias/missao9.txt`).

| Peça | RPO | RTO | Por que esses números | Como volta |
|---|---|---|---|---|
| **Origem** (`lab_origem`) | 24 h | 4 h | É o banco da loja: a cópia dele é responsabilidade do time dono (backup diário do banco). Entre um backup e outro, toda mudança está no log de mudanças | backup do banco + log de mudanças desde o backup |
| **Log de mudanças e tópico** (`lab_log`) | 0 | 30 min | É a fonte de verdade da recuperação: só se anexa, e cada evento entra assim que o banco confirma. Perder o log é perder o que não está em nenhuma cópia, por isso RPO 0 | Delta com histórico + cópia do log em outro lugar (próxima melhoria: exportar para o volume a cada execução) |
| **Bronze** (`lab_bronze`) | 0 | 15 min (junto com silver e gold) | Não precisa de cópia diária: volta da cópia do dia 1 + log + tópico + lista de eliminação + lotes guardados em `dados/`. Nada que chegou se perde (RPO 0) | `09c`: restaurar, reaplicar log e tópico, reaplicar a lista, recarregar os lotes |
| **Silver e gold** (`lab_silver`, `lab_gold`) | 0 | dentro dos 15 min | São derivadas: saem da bronze, do contrato e da chave. Não têm cópia; têm o comando `reconstruir` | `09b_reconstruir` |
| **Chave do pseudônimo** (`CHAVE_PSEUDONIMO`) | 0 | 5 min | Sem a chave, os pseudônimos novos não batem com os antigos (a lista de eliminação para de funcionar). Por isso três cópias, e a chave nunca muda sem decisão | secret scope `lab_bd`, cópia na pasta pessoal, cópia offline do dono |
| **Lista de eliminação** (`lab_controle.eliminacoes`) | 0 | junto com a bronze | Sem ela, restaurar uma cópia antiga traria de volta quem pediu para ser esquecido | tabela Delta + o registro em `evidencias/missao8.txt`; reaplicada em toda restauração |

**RTO prometido para o ensaio: 15 minutos** da bronze apagada até a visão de consumo de volta, com os mesmos números.
A diferença entre o prometido e o medido fica registrada na evidência e na seção "Resultado do ensaio" abaixo:
medimos 17,2 min, e o prometido foi revisto para 20 min até a validação deixar de subir um contexto novo.

## Esquema 3-2-1-1-0

| Regra | Como cumprimos |
|---|---|
| **3** cópias | (1) a bronze em produção; (2) `lab_copias.bronze_dia1_*`, tabelas Delta; (3) os mesmos dados em Parquet no volume `/Volumes/<catálogo>/lab_copias/cofre/dia1`, e a entrada `dados/dia1` no GitHub |
| **2** mídias diferentes | tabela Delta gerenciada pelo Unity Catalog × arquivos Parquet no volume × arquivos CSV num repositório Git |
| **1** fora do lugar | o repositório no GitHub: outro provedor, fora do Databricks |
| **1** imutável ou isolada | **imutável:** as tabelas de cópia têm `delta.appendOnly = true` (não aceitam UPDATE nem DELETE), e a tag `entrega-N` no GitHub congela o estado de cada entrega. **Isolada, com credencial independente:** o GitHub, acessado com a credencial do GitHub do time, que não é a do Databricks; quem perder o acesso ao workspace não apaga o repositório |
| **0** erros na restauração | o ensaio restaura de fato e confere os números (6 clientes, 11 pedidos, 6 eventos, R$ 844,90, mesma visão); só conta como cópia o que já foi restaurado com sucesso |

**Offsets junto com a cópia.** `lab_copias.metadados` guarda em que ponto do log de mudanças e do tópico cada cópia
foi tirada. A cópia do dia 1 é anterior a qualquer evento (offset 0), então a recuperação reaplica o log e o tópico
inteiros. Sem o offset, não se sabe o que reaplicar: reaplicar de menos perde dado, de mais pode desfazer mudanças.

**A chave em outro lugar.** A chave principal fica no secret scope. A cópia fica num arquivo na pasta pessoal do dono
no workspace (fora do Git folder, portanto fora do repositório), e a terceira cópia, offline, no gerenciador de senhas
do time, feita à mão. A chave nunca vai para o repositório, para o log ou para a evidência.

## Ordem da recuperação (ensaio do desastre)

1. Apagar a bronze (o desastre simulado).
2. Restaurar a cópia do dia 1.
3. Reaplicar o log de mudanças e o tópico a partir dos offsets da cópia.
4. **Reaplicar a lista de eliminação.** A cópia do dia 1 tem a cliente 5. A ordem importa: a lista vem depois do
   log, para tratar também o que o log trouxer de volta.
5. Carregar o dia 3 (o lote por arquivo).
6. Reconstruir silver e gold (`09b_reconstruir`).

## Resultado do ensaio (16/10/2026)

| Etapa | Tempo |
|---|---|
| 1. apagar a bronze | 2,3 s |
| 2. restaurar a cópia do dia 1 | 9,4 s |
| 3. reaplicar log (5 eventos) e tópico (6) | 14,8 s |
| 4. reaplicar a lista de eliminação | 41,1 s |
| 5. carregar o dia 3 | 4,5 s |
| 6. reconstruir silver e gold | 961,2 s (validação: 938,2 s) |
| **Total (RTO real)** | **1033 s = 17,2 min** |

**Prometido: 15 min. Medido: 17,2 min. Estouramos em 2,2 min.** Os números voltaram iguais (6 clientes, 11 pedidos,
6 eventos, R$ 844,90, mesma visão) e a cliente 5 não voltou, então a recuperação funcionou; o prazo não.

**Onde foi o tempo:** 93% na etapa 6, e quase todo ele na validação. O `reconstruir` roda o validador com
`dbutils.notebook.run`, que sobe um contexto novo de execução no serverless da Free Edition, e o workspace estava sob
carga ("heavy load"). A mesma validação rodada no notebook já aberto leva perto de 1 minuto. As etapas que dependem
só dos dados (1 a 5) levaram 72 s juntas.

**O que fazer com a diferença:**
1. Rodar a validação no mesmo contexto do ensaio (`%run`) em vez de `dbutils.notebook.run`, ou num job com computação
   já ligada. Isso ataca 90% do tempo.
2. Até lá, o RTO prometido passa a ser **20 minutos**, com 3 minutos de folga sobre o medido. Prometer o que o ensaio
   não mostrou seria só um número no papel.
3. Repetir o ensaio depois da mudança e registrar o novo tempo aqui.

## O que não está coberto

- Backup da origem: é do time dono do banco; aqui a origem é uma tabela do laboratório.
- Exportação contínua do log para o volume: hoje o log depende do histórico do Delta no mesmo workspace.
- Perda do workspace inteiro: voltaria pelo GitHub (código, contratos, `dados/`), mas o log e a lista de eliminação
  precisariam de uma cópia fora do Databricks; a lista vai em formato sem dado pessoal na evidência da Missão 8.
