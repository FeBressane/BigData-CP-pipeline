# Databricks notebook source
# MAGIC %md
# MAGIC # Missão 2 · Capturar cada mudança e aplicar na bronze
# MAGIC
# MAGIC Duas etapas, cada uma com o seu offset em `lab_controle.offsets`:
# MAGIC
# MAGIC | Etapa | Lê de | Escreve em | Offset |
# MAGIC |---|---|---|---|
# MAGIC | **Captura** | Change Data Feed de `lab_origem.clientes` e `lab_origem.pedidos` | `lab_log.mudancas` (só anexa) | última versão Delta lida de cada tabela |
# MAGIC | **Aplicação** | `lab_log.mudancas` | `lab_bronze.clientes` e `lab_bronze.pedidos` | último `seq` aplicado |
# MAGIC
# MAGIC * Cada evento do log tem sequência, tabela, operação, chave e dados.
# MAGIC * Exclusão na origem vira exclusão na bronze.
# MAGIC * Reaplicar um trecho já aplicado não muda o resultado: para cada chave vale o último evento,
# MAGIC   e o `MERGE` insere, substitui ou apaga conforme a operação.
# MAGIC * Widget **Reaplicar desde o início** = `sim`: volta o offset da bronze a 0 e reaplica o log inteiro.

# COMMAND ----------

dbutils.widgets.text("catalogo", "workspace", "Catálogo")
dbutils.widgets.dropdown("reaplicar", "nao", ["nao", "sim"], "Reaplicar desde o início?")
CATALOGO = dbutils.widgets.get("catalogo")
REAPLICAR = dbutils.widgets.get("reaplicar") == "sim"
ORIGEM, BRONZE, CONTROLE, LOGS = (f"{CATALOGO}.lab_origem", f"{CATALOGO}.lab_bronze",
                                  f"{CATALOGO}.lab_controle", f"{CATALOGO}.lab_log")
LOG = f"{LOGS}.mudancas"
TABELAS = ["clientes", "pedidos"]

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CONTROLE}.sincronizacoes (
  execucao        INT,
  iniciada_em     TIMESTAMP,
  reaplicou       BOOLEAN,
  capturados      BIGINT,
  aplicados       BIGINT,
  offset_antes    BIGINT,
  offset_depois   BIGINT,
  bronze_clientes BIGINT,
  bronze_pedidos  BIGINT,
  receita_paga_centavos BIGINT
) COMMENT 'Uma linha por execução da sincronização pelo log'
""")


def ler_offset(consumidor: str, fonte: str):
    r = spark.sql(f"SELECT posicao FROM {CONTROLE}.offsets "
                  f"WHERE consumidor = '{consumidor}' AND fonte = '{fonte}'").first()
    return r.posicao if r else None


def gravar_offset(consumidor: str, fonte: str, posicao: int) -> None:
    spark.sql(f"""
      MERGE INTO {CONTROLE}.offsets o
      USING (SELECT '{consumidor}' AS consumidor, '{fonte}' AS fonte, {posicao} AS posicao) n
      ON o.consumidor = n.consumidor AND o.fonte = n.fonte
      WHEN MATCHED THEN UPDATE SET o.posicao = n.posicao, o.atualizado_em = current_timestamp()
      WHEN NOT MATCHED THEN INSERT (consumidor, fonte, posicao, atualizado_em)
        VALUES (n.consumidor, n.fonte, n.posicao, current_timestamp())
    """)


from datetime import datetime
EXECUCAO = spark.sql(f"SELECT COALESCE(MAX(execucao), 0) + 1 AS n FROM {CONTROLE}.sincronizacoes").first().n
INICIO = datetime.now()
print(f"Sincronização {EXECUCAO} · {INICIO:%Y-%m-%d %H:%M:%S}" + (" · REAPLICANDO desde o início" if REAPLICAR else ""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Etapa 1 · Captura: Change Data Feed → log

# COMMAND ----------

consultas, ultima_versao = [], {}
for t in TABELAS:
    inicio = ler_offset("captura", t)
    assert inicio is not None, "Offset da captura não encontrado. Rodem 02a_preparar_captura antes."
    atual = spark.sql(f"DESCRIBE HISTORY {ORIGEM}.{t} LIMIT 1").first().version
    if atual <= inicio:
        continue  # nada novo nesta tabela
    colunas = ", ".join(spark.table(f"{ORIGEM}.{t}").columns)
    consultas.append(f"""
      SELECT '{t}' AS tabela,
             CASE _change_type WHEN 'insert' THEN 'insercao'
                               WHEN 'update_postimage' THEN 'alteracao'
                               WHEN 'delete' THEN 'exclusao' END AS operacao,
             id AS chave,
             to_json(struct({colunas})) AS dados,
             _commit_version AS versao_origem,
             _commit_timestamp AS quando
      FROM table_changes('{ORIGEM}.{t}', {inicio + 1})
      WHERE _change_type <> 'update_preimage'""")
    ultima_versao[t] = atual

capturados = 0
if consultas:
    spark.sql(" UNION ALL ".join(consultas)).createOrReplaceTempView("_eventos_novos")
    capturados = spark.table("_eventos_novos").count()
    base = spark.sql(f"SELECT COALESCE(MAX(seq), 0) AS m FROM {LOG}").first().m
    spark.sql(f"""
      INSERT INTO {LOG}
      SELECT {base} + ROW_NUMBER() OVER (ORDER BY quando, tabela, versao_origem) AS seq,
             tabela, operacao, chave, dados, versao_origem, quando, current_timestamp()
      FROM _eventos_novos
    """)
for t, v in ultima_versao.items():
    gravar_offset("captura", t, v)
print(f"Captura: {capturados} evento(s) novo(s) anexado(s) ao log")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Etapa 2 · Aplicação: log → bronze, em ordem

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

offset_antes = 0 if REAPLICAR else (ler_offset("bronze", "mudancas") or 0)
novos = spark.table(LOG).where(F.col("seq") > offset_antes)
aplicados = novos.count()

if aplicados:
    for t in TABELAS:
        esquema = spark.table(f"{ORIGEM}.{t}").schema
        colunas = [c.name for c in esquema]
        # aplicar em ordem = para cada chave, o resultado é o do último evento
        ultimos = (novos.where(F.col("tabela") == t)
                        .withColumn("_n", F.row_number().over(Window.partitionBy("chave").orderBy(F.col("seq").desc())))
                        .where("_n = 1")
                        .select("seq", "operacao", F.from_json("dados", esquema).alias("d"))
                        .select("seq", "operacao", "d.*"))
        if ultimos.isEmpty():
            continue
        ultimos.createOrReplaceTempView("_ultimos")
        atualiza = ", ".join(f"b.{c} = n.{c}" for c in colunas)
        spark.sql(f"""
          MERGE INTO {BRONZE}.{t} b
          USING (SELECT *, current_timestamp() AS _carregado_em, {EXECUCAO} AS _execucao FROM _ultimos) n
          ON b.id = n.id
          WHEN MATCHED AND n.operacao = 'exclusao' THEN DELETE
          WHEN MATCHED THEN UPDATE SET {atualiza}, b._carregado_em = n._carregado_em, b._execucao = n._execucao
          WHEN NOT MATCHED AND n.operacao <> 'exclusao' THEN
            INSERT ({", ".join(colunas)}, _carregado_em, _execucao)
            VALUES ({", ".join("n." + c for c in colunas)}, n._carregado_em, n._execucao)
        """)

offset_depois = spark.sql(f"SELECT COALESCE(MAX(seq), 0) AS m FROM {LOG}").first().m
gravar_offset("bronze", "mudancas", offset_depois)
print(f"Aplicação: {aplicados} evento(s) aplicados (offset {offset_antes} → {offset_depois})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resultado

# COMMAND ----------

r = spark.sql(f"""
SELECT
  (SELECT COUNT(*) FROM {BRONZE}.clientes) AS clientes,
  (SELECT COUNT(*) FROM {BRONZE}.pedidos)  AS pedidos,
  (SELECT COALESCE(SUM(valor_centavos), 0) FROM {BRONZE}.pedidos WHERE status = 'pago') AS receita
""").first()

spark.sql(f"""
INSERT INTO {CONTROLE}.sincronizacoes VALUES (
  {EXECUCAO}, TIMESTAMP '{INICIO:%Y-%m-%d %H:%M:%S}', {str(REAPLICAR).lower()},
  {capturados}, {aplicados}, {offset_antes}, {offset_depois}, {r.clientes}, {r.pedidos}, {r.receita}
)""")

print(f"Bronze: {r.clientes} clientes, {r.pedidos} pedidos, receita paga R$ {r.receita / 100:.2f}".replace(".", ","))
display(spark.sql(f"SELECT seq, tabela, operacao, chave, versao_origem, quando FROM {LOG} ORDER BY seq"))
