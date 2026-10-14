# Databricks notebook source
# MAGIC %md
# MAGIC # Missão 3 · Leitor rápido `visitas`
# MAGIC
# MAGIC Lê cada evento assim que chega (acorda a cada meio segundo) e mantém a contagem de visitas por página
# MAGIC em `lab_leitores.visitas_por_pagina`.
# MAGIC
# MAGIC **Idempotente:** cada evento contado fica registrado pelo `evento_id` em `lab_leitores.visitas_eventos`.
# MAGIC Se o leitor cair e ler de novo o mesmo evento, ele o reconhece e não conta duas vezes.
# MAGIC
# MAGIC **Teste da queda:** com o widget "Cair no evento" = `4`, o leitor processa até o 4º evento e cai
# MAGIC **antes** de salvar o offset (a célula termina em erro, de propósito). Quando o produtor terminar,
# MAGIC mudem o widget para `0` e rodem de novo: ele retoma do offset salvo e chega aos mesmos totais.

# COMMAND ----------

# MAGIC %run ./03_comum

# COMMAND ----------

dbutils.widgets.dropdown("cair_no", "0", [str(i) for i in range(0, 9)], "Cair no evento (0 = não cair)")
CAIR_NO = int(dbutils.widgets.get("cair_no"))


def processar_visitas(trecho, rodada):
    trecho.createOrReplaceTempView("_trecho")
    n = trecho.count()
    rep = spark.sql(f"""SELECT COUNT(*) AS r FROM _trecho t
                        WHERE EXISTS (SELECT 1 FROM {LEITORES}.visitas_eventos v WHERE v.evento_id = t.evento_id)""").first().r
    spark.sql(f"""
      MERGE INTO {LEITORES}.visitas_eventos v
      USING (SELECT evento_id, pagina FROM _trecho) n
      ON v.evento_id = n.evento_id
      WHEN NOT MATCHED THEN INSERT (evento_id, pagina, contado_em) VALUES (n.evento_id, n.pagina, current_timestamp())
    """)
    return n, rep


rodar_leitor("visitas", processar_visitas, intervalo=0.5, ocioso_max=20, cair_no_seq=CAIR_NO)

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {LEITORES}.visitas_por_pagina ORDER BY visitas DESC, pagina"))
