# Databricks notebook source
# MAGIC %md
# MAGIC # Missão 7 · Pseudônimo não é anônimo: medir o K e montar a visão de consumo
# MAGIC
# MAGIC 1. **Mede o K** da tabela de clientes da silver pelos quase-identificadores `cidade` + `faixa_etaria`: quantas
# MAGIC    pessoas há em cada combinação, qual é o menor grupo (K) e quantas combinações têm uma pessoa só. O resultado
# MAGIC    fica em `lab_controle.medicoes_k`.
# MAGIC 2. **Cria a visão gold** `lab_gold.receita_por_cidade` (cidade, número de clientes, receita paga) com o limiar
# MAGIC    K do widget: cidade com menos de K clientes não aparece pelo nome.
# MAGIC
# MAGIC Rodem antes `qualidade/04b_validar`, que gera a silver com a faixa etária no lugar do nascimento.

# COMMAND ----------

# MAGIC %run ./07_consumo_comum

# COMMAND ----------

dbutils.widgets.text("catalogo", "workspace", "Catálogo")
dbutils.widgets.text("k", "2", "Limiar K da visão")
CATALOGO = dbutils.widgets.get("catalogo")
K = int(dbutils.widgets.get("k"))
SILVER, GOLD, CONTROLE = f"{CATALOGO}.lab_silver", f"{CATALOGO}.lab_gold", f"{CATALOGO}.lab_controle"

colunas = spark.table(f"{SILVER}.clientes").columns
assert "faixa_etaria" in colunas and "nascimento" not in colunas, \
    "A silver ainda tem o nascimento. Rodem qualidade/04b_validar antes deste notebook."

k_medido, unicas, grupos = medir_k(SILVER)
print(f"Quase-identificadores: cidade + faixa_etaria · {len(grupos)} combinações · K = {k_medido} · "
      f"{unicas} combinação(ões) com uma pessoa só")
display(spark.createDataFrame(grupos))

spark.sql(f"""CREATE TABLE IF NOT EXISTS {CONTROLE}.medicoes_k (
  medido_em TIMESTAMP, tabela STRING, quase_identificadores STRING, combinacoes INT, k INT, combinacoes_unicas INT
) COMMENT 'Medições de K-anonimato da silver (Missão 7)'""")
spark.sql(f"""INSERT INTO {CONTROLE}.medicoes_k VALUES (current_timestamp(), 'lab_silver.clientes',
              'cidade, faixa_etaria', {len(grupos)}, {k_medido}, {unicas})""")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD}")
spark.sql(f"""
CREATE OR REPLACE VIEW {GOLD}.receita_por_cidade
COMMENT 'Receita paga por cidade. Cidade com menos de {K} clientes é suprimida (limiar K = {K}); a receita dela entra na linha de outras, junto com os pedidos sem cliente.'
AS {sql_receita_por_cidade(SILVER, K)}
""")
print(f"Visão {GOLD}.receita_por_cidade criada com K = {K}")
display(spark.table(f"{GOLD}.receita_por_cidade"))
