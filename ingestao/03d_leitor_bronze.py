# Databricks notebook source
# MAGIC %md
# MAGIC # Missão 3 · Bronze como leitor lento
# MAGIC
# MAGIC A bronze se liga ao mesmo tópico, mas acorda **a cada 5 segundos** e grava em rajada tudo o que chegou
# MAGIC desde o seu offset. Durante a publicação ela fica alguns eventos atrás do leitor rápido, e o atraso de
# MAGIC um não segura o outro.
# MAGIC
# MAGIC A gravação é por mesclagem pelo `evento_id`: ler de novo um trecho não duplica nada.

# COMMAND ----------

# MAGIC %run ./03_comum

# COMMAND ----------

COLUNAS = ["evento_id", "cliente_id", "ip", "pagina", "quando"]


def processar_bronze(trecho, rodada):
    trecho.createOrReplaceTempView("_trecho")
    n = trecho.count()
    rep = spark.sql(f"""SELECT COUNT(*) AS r FROM _trecho t
                        WHERE EXISTS (SELECT 1 FROM {BRONZE}.navegacao b WHERE b.evento_id = t.evento_id)""").first().r
    spark.sql(f"""
      MERGE INTO {BRONZE}.navegacao b
      USING (SELECT {", ".join(COLUNAS)}, current_timestamp() AS _carregado_em, {rodada} AS _execucao FROM _trecho) n
      ON b.evento_id = n.evento_id
      WHEN NOT MATCHED THEN INSERT ({", ".join(COLUNAS)}, _carregado_em, _execucao)
        VALUES ({", ".join("n." + c for c in COLUNAS)}, n._carregado_em, n._execucao)
    """)
    return n, rep


rodar_leitor("bronze", processar_bronze, intervalo=5, ocioso_max=20)

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {BRONZE}.navegacao ORDER BY evento_id"))
