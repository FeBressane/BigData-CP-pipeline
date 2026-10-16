# Databricks notebook source
# MAGIC %md
# MAGIC # Missão 8 · esquecer &lt;id&gt;
# MAGIC
# MAGIC Atende um pedido de eliminação: trata todas as moradas do cliente (origem, log de mudanças, tópico de
# MAGIC navegação, estado dos leitores, bronze, quarentena e silver), deixa os pedidos dele **sem pessoa** e registra o
# MAGIC pedido na lista de eliminação, sem dado pessoal. Inventário das moradas: `privacidade/moradas.md`.
# MAGIC
# MAGIC Uso: widget **Cliente a esquecer** = `5` e Run all. Rodar de novo não falha nem muda nada.
# MAGIC
# MAGIC Depois, rodem `qualidade/04b_validar`: a silver refeita da bronze tem que dar os mesmos números.

# COMMAND ----------

# MAGIC %run ../seguranca/06a_protecao

# COMMAND ----------

# MAGIC %run ./08_esquecer_comum

# COMMAND ----------

dbutils.widgets.text("catalogo", "workspace", "Catálogo")
dbutils.widgets.text("cliente_id", "5", "Cliente a esquecer")
dbutils.widgets.dropdown("limpar_historico", "sim", ["sim", "nao"], "Limpar histórico do Delta?")
CATALOGO = dbutils.widgets.get("catalogo")
CLIENTE = int(dbutils.widgets.get("cliente_id"))

r = esquecer(CATALOGO, CLIENTE, limpar=dbutils.widgets.get("limpar_historico") == "sim")

print(f"esquecer {CLIENTE} · chave na lista: {r['chave_ps'][:12]}… · "
      f"{'já estava na lista' if r['ja_estava_na_lista'] else 'registrado na lista de eliminação'}")
print("\nMoradas tratadas (linhas):")
for morada, n in r["moradas"].items():
    print(f"  {morada:<45} {n}")
print(f"  total: {sum(r['moradas'].values())} linhas")
if r["historico"]:
    print("\nHistórico do Delta (versões antigas também guardam o dado):")
    for t, st in r["historico"].items():
        print(f"  {t:<45} {st}")
