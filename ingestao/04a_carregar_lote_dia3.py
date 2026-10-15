# Databricks notebook source
# MAGIC %md
# MAGIC # Missão 4 · Carregar o lote do dia 3 na bronze
# MAGIC
# MAGIC No dia 3 chega um lote de pedidos **por arquivo** (`dados/dia3/pedidos_dia3.csv`), e ele tem defeitos.
# MAGIC A bronze guarda o lote **como chegou**: todas as colunas como texto, linha por linha, sem corrigir
# MAGIC nada e sem descartar nada. Quem decide o que segue para a silver é a validação (`qualidade/04b_validar`).
# MAGIC
# MAGIC As linhas ficam em `lab_bronze.pedidos_lote`, com o nome do arquivo e o número da linha. Carregar
# MAGIC o mesmo arquivo de novo não duplica nada (mesclagem por arquivo + linha).

# COMMAND ----------

dbutils.widgets.text("catalogo", "workspace", "Catálogo")
CATALOGO = dbutils.widgets.get("catalogo")
BRONZE, CONTROLE = f"{CATALOGO}.lab_bronze", f"{CATALOGO}.lab_controle"
ARQUIVO = "pedidos_dia3.csv"

import os
import pandas as pd

caminho = os.path.abspath(os.path.join(os.getcwd(), "..", "dados", "dia3", ARQUIVO))
pdf = pd.read_csv(caminho, dtype=str, keep_default_na=False)
pdf.insert(0, "_linha", range(1, len(pdf) + 1))
print(f"{ARQUIVO}: {len(pdf)} linhas")
display(pdf)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {BRONZE}.pedidos_lote (
  id STRING, cliente_id STRING, valor_centavos STRING, status STRING, criado_em STRING, observacao STRING,
  _arquivo STRING, _linha INT, _carregado_em TIMESTAMP, _execucao INT
) COMMENT 'Lotes de pedidos recebidos por arquivo, como chegaram (tudo como texto)'
""")

EXECUCAO = spark.sql(f"SELECT COALESCE(MAX(_execucao), 0) + 1 AS n FROM {BRONZE}.pedidos_lote").first().n

spark.createDataFrame(pdf.astype({"_linha": int})).createOrReplaceTempView("_lote")
spark.sql(f"""
  MERGE INTO {BRONZE}.pedidos_lote b
  USING (SELECT id, cliente_id, valor_centavos, status, criado_em, observacao,
                '{ARQUIVO}' AS _arquivo, CAST(_linha AS INT) AS _linha,
                current_timestamp() AS _carregado_em, {EXECUCAO} AS _execucao
         FROM _lote) n
  ON b._arquivo = n._arquivo AND b._linha = n._linha
  WHEN NOT MATCHED THEN INSERT *
""")

n = spark.sql(f"SELECT COUNT(*) AS n FROM {BRONZE}.pedidos_lote WHERE _arquivo = '{ARQUIVO}'").first().n
print(f"lab_bronze.pedidos_lote: {n} linhas do {ARQUIVO} (esperado 7)")
assert n == 7
