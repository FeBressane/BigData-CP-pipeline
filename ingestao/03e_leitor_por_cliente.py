# Databricks notebook source
# MAGIC %md
# MAGIC # Missão 3 · Leitor novo `por_cliente`
# MAGIC
# MAGIC Ligado **depois** que tudo já foi publicado. Ele não tem offset ainda, então começa do offset 0 e
# MAGIC "assiste à história inteira": conta os eventos por cliente desde o primeiro.
# MAGIC
# MAGIC Nada no produtor nem nos outros leitores foi alterado para ele existir. É a ideia do log:
# MAGIC um destino novo se liga sozinho.

# COMMAND ----------

# MAGIC %run ./03_comum

# COMMAND ----------

def processar_por_cliente(trecho, rodada):
    trecho.createOrReplaceTempView("_trecho")
    n = trecho.count()
    rep = spark.sql(f"""SELECT COUNT(*) AS r FROM _trecho t
                        WHERE EXISTS (SELECT 1 FROM {LEITORES}.por_cliente_eventos p WHERE p.evento_id = t.evento_id)""").first().r
    spark.sql(f"""
      MERGE INTO {LEITORES}.por_cliente_eventos p
      USING (SELECT evento_id, cliente_id FROM _trecho) n
      ON p.evento_id = n.evento_id
      WHEN NOT MATCHED THEN INSERT (evento_id, cliente_id, contado_em) VALUES (n.evento_id, n.cliente_id, current_timestamp())
    """)
    return n, rep


rodar_leitor("por_cliente", processar_por_cliente, intervalo=1, ocioso_max=5, espera_inicial=10)

# COMMAND ----------

display(spark.sql(f"""SELECT COALESCE(CAST(cliente_id AS STRING), 'visitante sem cadastro') AS cliente, eventos
                      FROM {LEITORES}.eventos_por_cliente ORDER BY cliente_id NULLS LAST"""))
