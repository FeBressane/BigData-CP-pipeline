# Databricks notebook source
# MAGIC %md
# MAGIC # Missão 9 · reconstruir
# MAGIC
# MAGIC Apaga a silver (`clientes`, `pedidos`, `navegacao`) e a visão de consumo (`lab_gold.receita_por_cidade`) e recria
# MAGIC tudo do zero a partir da bronze, rodando o validador (`qualidade/04b_validar`) e a visão (`privacidade/07a`).
# MAGIC Nada da silver ou da gold precisa de cópia: tudo sai da bronze, do contrato e da chave.

# COMMAND ----------

# MAGIC %run ../seguranca/06a_protecao

# COMMAND ----------

# MAGIC %run ../privacidade/08_esquecer_comum

# COMMAND ----------

# MAGIC %run ./09_resiliencia_comum

# COMMAND ----------

dbutils.widgets.text("catalogo", "workspace", "Catálogo")
CATALOGO = dbutils.widgets.get("catalogo")

antes = numeros(CATALOGO)
tempos = reconstruir(CATALOGO)
depois = numeros(CATALOGO)

for etapa, s in tempos.items():
    print(f"  {etapa:<32} {s:6.1f} s")
print(f"  {'total':<32} {sum(tempos.values()):6.1f} s")
print("\nAntes: ", antes)
print("Depois:", depois)
print("Mesmos números:", "sim" if antes == depois else "NÃO")
