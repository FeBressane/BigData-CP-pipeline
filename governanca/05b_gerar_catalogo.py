# Databricks notebook source
# MAGIC %md
# MAGIC # Missão 5 · Gerar o catálogo
# MAGIC
# MAGIC Gera `governanca/catalogo.md` a partir dos contratos, do acordo e dos metadados das tabelas (linhas e última
# MAGIC alteração, lidos da plataforma). Se o acordo não for cumprido, a regra como código para antes de gerar.
# MAGIC Depois de rodar, façam commit do `governanca/catalogo.md`.

# COMMAND ----------

# MAGIC %run ./05a_governanca

# COMMAND ----------

from datetime import datetime

dbutils.widgets.text("catalogo", "workspace", "Catálogo")
CATALOGO = dbutils.widgets.get("catalogo")

# Tabelas de cada conjunto, por camada (as que existirem entram no catálogo)
TABELAS = {
    "pedidos": {"bronze": f"{CATALOGO}.lab_bronze.pedidos", "silver": f"{CATALOGO}.lab_silver.pedidos"},
    "clientes": {"bronze": f"{CATALOGO}.lab_bronze.clientes", "silver": f"{CATALOGO}.lab_silver.clientes"},
    "navegacao": {"bronze": f"{CATALOGO}.lab_bronze.navegacao", "silver": f"{CATALOGO}.lab_silver.navegacao"},
}


def ler_metadados() -> dict:
    meta = {}
    for conjunto, camadas in TABELAS.items():
        for camada, tabela in camadas.items():
            if not spark.catalog.tableExists(tabela):
                continue
            det = spark.sql(f"DESCRIBE DETAIL {tabela}").first()
            n = spark.table(tabela).count()
            meta.setdefault(conjunto, {})[camada] = {
                "tabela": tabela, "linhas": n, "alterada_em": f"{det.lastModified:%Y-%m-%d %H:%M}"}
    return meta


contratos, acordo = ler_contratos(), ler_acordo()
texto = gerar_catalogo(contratos, acordo, ler_metadados(), gerado_em=f"{datetime.now():%Y-%m-%d %H:%M}")
caminho = os.path.join(RAIZ_REPO, "governanca", "catalogo.md")
with open(caminho, "w", encoding="utf-8") as f:
    f.write(texto)
print(texto)
print("\nCatálogo gravado em", caminho)
