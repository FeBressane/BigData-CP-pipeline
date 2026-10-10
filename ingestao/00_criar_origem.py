# Databricks notebook source
# MAGIC %md
# MAGIC # Missão 0 · A origem
# MAGIC
# MAGIC Cria o banco de origem da loja (schema `lab_origem`) e carrega os dados do dia 1.
# MAGIC
# MAGIC * As tabelas `clientes`, `pedidos` e `navegacao` **não têm chave estrangeira** entre si:
# MAGIC   o pedido precisa sobreviver à exclusão do cliente (Missão 2 e Missão 8).
# MAGIC * Rodar de novo recria a origem do zero no estado do dia 1.
# MAGIC * Também cria os schemas das camadas que as próximas missões vão usar.

# COMMAND ----------

dbutils.widgets.text("catalogo", "workspace", "Catálogo")
CATALOGO = dbutils.widgets.get("catalogo")

SCHEMAS = {
    "lab_origem":   "Origem da loja: o banco transacional que o pipeline lê",
    "lab_controle": "Estado do pipeline: marcas-d'água, offsets, execuções",
    "lab_bronze":   "Camada bronze: cópia fiel da origem",
    "lab_silver":   "Camada silver: validada e protegida",
    "lab_gold":     "Camada gold: consumo agregado",
}
for nome, comentario in SCHEMAS.items():
    # escapa a aspa simples (marcas-d'água) para não fechar a string do SQL
    texto = comentario.replace("\\", "\\\\").replace("'", "\\'")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{nome} COMMENT '{texto}'")

ORIGEM = f"{CATALOGO}.lab_origem"
print("Schemas prontos em", CATALOGO)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabelas da origem

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {ORIGEM}.clientes (
  id            INT       NOT NULL,
  nome          STRING,
  email         STRING,
  cpf           STRING,
  nascimento    DATE,
  cidade        STRING,
  atualizado_em TIMESTAMP NOT NULL
) COMMENT 'Clientes da loja (dado de banco: pequeno, precioso, muda de valor)'
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {ORIGEM}.pedidos (
  id             INT       NOT NULL,
  cliente_id     INT,
  valor_centavos INT,
  status         STRING,
  criado_em      TIMESTAMP,
  atualizado_em  TIMESTAMP NOT NULL,
  observacao     STRING
) COMMENT 'Pedidos. Sem chave estrangeira para clientes: o pedido sobrevive à exclusão do cliente'
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {ORIGEM}.navegacao (
  evento_id  INT       NOT NULL,
  cliente_id INT,
  ip         STRING,
  pagina     STRING,
  quando     TIMESTAMP
) COMMENT 'Eventos de navegação (dado de atividade: volumoso, só acrescenta)'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carga do dia 1
# MAGIC Os CSVs ficam em `dados/dia1/` no próprio repositório.

# COMMAND ----------

import os
import pandas as pd

PASTA_DIA1 = os.path.abspath(os.path.join(os.getcwd(), "..", "dados", "dia1"))

def carregar(arquivo: str, tabela: str, select_sql: str) -> None:
    pdf = pd.read_csv(os.path.join(PASTA_DIA1, arquivo), dtype=str, keep_default_na=False)
    spark.createDataFrame(pdf).createOrReplaceTempView("_csv")
    spark.sql(f"INSERT INTO {ORIGEM}.{tabela} {select_sql}")

carregar("clientes.csv", "clientes", """
  SELECT CAST(id AS INT), nome, email, cpf, CAST(nascimento AS DATE), cidade,
         CAST(atualizado_em AS TIMESTAMP)
  FROM _csv""")

carregar("pedidos.csv", "pedidos", """
  SELECT CAST(id AS INT), CAST(NULLIF(cliente_id, '') AS INT), CAST(valor_centavos AS INT),
         status, CAST(criado_em AS TIMESTAMP), CAST(atualizado_em AS TIMESTAMP),
         NULLIF(observacao, '')
  FROM _csv""")

carregar("navegacao.csv", "navegacao", """
  SELECT CAST(evento_id AS INT), CAST(NULLIF(cliente_id, '') AS INT), ip, pagina,
         CAST(quando AS TIMESTAMP)
  FROM _csv""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Confiram na origem
# MAGIC Esperado: **6 clientes, 8 pedidos, 8 eventos** e **receita paga de R$ 655,00**.

# COMMAND ----------

from datetime import datetime

r = spark.sql(f"""
SELECT
  (SELECT COUNT(*) FROM {ORIGEM}.clientes)  AS clientes,
  (SELECT COUNT(*) FROM {ORIGEM}.pedidos)   AS pedidos,
  (SELECT COUNT(*) FROM {ORIGEM}.navegacao) AS eventos,
  (SELECT SUM(valor_centavos) FROM {ORIGEM}.pedidos WHERE status = 'pago') AS receita_paga_centavos
""").first()

receita = f"R$ {r.receita_paga_centavos / 100:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
esperado = (r.clientes, r.pedidos, r.eventos, r.receita_paga_centavos) == (6, 8, 8, 65500)

linhas = [
    f"Missão 0 · origem no estado do dia 1 · {datetime.now():%Y-%m-%d %H:%M}",
    f"catálogo: {CATALOGO} · schema: lab_origem",
    f"clientes: {r.clientes} (esperado 6)",
    f"pedidos:  {r.pedidos} (esperado 8)",
    f"eventos de navegação: {r.eventos} (esperado 8)",
    f"receita paga: {receita} (esperado R$ 655,00)",
    "resultado: " + ("OK, números conferem" if esperado else "DIVERGENTE, revisar a carga"),
]
texto = "\n".join(linhas)
print(texto)

caminho = os.path.abspath(os.path.join(os.getcwd(), "..", "evidencias", "missao0.txt"))
os.makedirs(os.path.dirname(caminho), exist_ok=True)
with open(caminho, "w", encoding="utf-8") as f:
    f.write(texto + "\n")
print("\nEvidência gravada em", caminho)
assert esperado, "Os números da origem não batem com o laboratório"
