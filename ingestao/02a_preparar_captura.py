# Databricks notebook source
# MAGIC %md
# MAGIC # Missão 2 · Preparar a captura de mudanças
# MAGIC
# MAGIC **Trilha B:** o próprio banco conta o que mudou. Ligamos o *Change Data Feed* (CDF) do Delta
# MAGIC nas tabelas `clientes` e `pedidos` da origem. A partir daí, cada INSERT, UPDATE e DELETE
# MAGIC fica registrado no log de transações da tabela, na ordem do commit.
# MAGIC
# MAGIC Este notebook deixa tudo no ponto de partida do exercício:
# MAGIC 1. confere que a origem está no **estado do dia 1** (rodem `00_criar_origem` antes);
# MAGIC 2. liga o CDF e guarda a versão atual de cada tabela como **offset da captura**;
# MAGIC 3. volta a **bronze** de `clientes` e `pedidos` ao estado do dia 1;
# MAGIC 4. zera o log (`lab_log.mudancas`) e o offset do consumidor da bronze.
# MAGIC
# MAGIC **Ordem do exercício:** `00_criar_origem` → **este** → `01b_aplicar_dia2` →
# MAGIC `02_sincronizar` → `02_sincronizar` com "Reaplicar desde o início" = `sim` → `02c_verificar_missao2`.

# COMMAND ----------

dbutils.widgets.text("catalogo", "workspace", "Catálogo")
CATALOGO = dbutils.widgets.get("catalogo")
ORIGEM, BRONZE, CONTROLE, LOGS = (f"{CATALOGO}.lab_origem", f"{CATALOGO}.lab_bronze",
                                  f"{CATALOGO}.lab_controle", f"{CATALOGO}.lab_log")
TABELAS = ["clientes", "pedidos"]

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {LOGS} COMMENT 'Log de mudanças: só se anexa no fim, lido pelo offset'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · A origem está no dia 1?

# COMMAND ----------

r = spark.sql(f"""
SELECT
  (SELECT COUNT(*) FROM {ORIGEM}.clientes) AS clientes,
  (SELECT COUNT(*) FROM {ORIGEM}.pedidos)  AS pedidos,
  (SELECT COUNT(*) FROM {ORIGEM}.clientes WHERE id = 5) AS tem_cliente_5,
  (SELECT COUNT(*) FROM {ORIGEM}.pedidos WHERE id = 109) AS tem_pedido_109
""").first()
print(f"Origem: {r.clientes} clientes, {r.pedidos} pedidos")
assert (r.clientes, r.pedidos, r.tem_cliente_5, r.tem_pedido_109) == (6, 8, 1, 0), \
    "A origem não está no dia 1. Rodem 00_criar_origem antes deste notebook."

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Ligar o Change Data Feed e marcar o ponto de partida

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CONTROLE}.offsets (
  consumidor    STRING NOT NULL,
  fonte         STRING NOT NULL,
  posicao       BIGINT,
  atualizado_em TIMESTAMP
) COMMENT 'Posição de cada leitor do log: o único estado do consumidor'
""")


def gravar_offset(consumidor: str, fonte: str, posicao: int) -> None:
    spark.sql(f"""
      MERGE INTO {CONTROLE}.offsets o
      USING (SELECT '{consumidor}' AS consumidor, '{fonte}' AS fonte, {posicao} AS posicao) n
      ON o.consumidor = n.consumidor AND o.fonte = n.fonte
      WHEN MATCHED THEN UPDATE SET o.posicao = n.posicao, o.atualizado_em = current_timestamp()
      WHEN NOT MATCHED THEN INSERT (consumidor, fonte, posicao, atualizado_em)
        VALUES (n.consumidor, n.fonte, n.posicao, current_timestamp())
    """)


for t in TABELAS:
    spark.sql(f"ALTER TABLE {ORIGEM}.{t} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
    versao = spark.sql(f"DESCRIBE HISTORY {ORIGEM}.{t} LIMIT 1").first().version
    gravar_offset("captura", t, versao)  # a captura lê da versão seguinte em diante
    print(f"CDF ligado em {t}: a captura começa depois da versão {versao}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Bronze de volta ao dia 1 e log vazio

# COMMAND ----------

for t in TABELAS:
    spark.sql(f"""
      CREATE OR REPLACE TABLE {BRONZE}.{t} AS
      SELECT *, current_timestamp() AS _carregado_em, CAST(0 AS INT) AS _execucao
      FROM {ORIGEM}.{t}
    """)

spark.sql(f"DROP TABLE IF EXISTS {LOGS}.mudancas")
spark.sql(f"""
CREATE TABLE {LOGS}.mudancas (
  seq           BIGINT    COMMENT 'Posição no log: ordem global das mudanças',
  tabela        STRING,
  operacao      STRING    COMMENT 'insercao | alteracao | exclusao',
  chave         INT,
  dados         STRING    COMMENT 'A linha em JSON (na exclusão, como era antes)',
  versao_origem BIGINT    COMMENT 'Versão Delta da tabela de origem que gerou o evento',
  quando        TIMESTAMP COMMENT 'Horário do commit na origem',
  capturado_em  TIMESTAMP
) COMMENT 'Log de mudanças de clientes e pedidos. Só se anexa no fim; nunca se edita.'
""")

spark.sql(f"DROP TABLE IF EXISTS {CONTROLE}.sincronizacoes")
gravar_offset("bronze", "mudancas", 0)

r = spark.sql(f"""
SELECT (SELECT COUNT(*) FROM {BRONZE}.clientes) AS c, (SELECT COUNT(*) FROM {BRONZE}.pedidos) AS p,
       (SELECT COUNT(*) FROM {LOGS}.mudancas) AS l
""").first()
print(f"Bronze no dia 1: {r.c} clientes, {r.p} pedidos · log com {r.l} eventos")
assert (r.c, r.p, r.l) == (6, 8, 0)
display(spark.sql(f"SELECT * FROM {CONTROLE}.offsets ORDER BY consumidor, fonte"))
