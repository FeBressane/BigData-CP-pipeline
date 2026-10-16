# Databricks notebook source
# MAGIC %md
# MAGIC # Missão 7 · Funções da visão de consumo
# MAGIC
# MAGIC Usadas por `07a_visao_consumo` e por `07b_verificar_missao7`. Não gravam nada.
# MAGIC
# MAGIC * `medir_k(quase_identificadores)`: agrupa os clientes da silver pelos quase-identificadores e devolve o
# MAGIC   tamanho de cada grupo. **K** é o menor grupo: cada pessoa se confunde com pelo menos K−1 outras.
# MAGIC * `sql_receita_por_cidade(k)`: receita paga por cidade, a partir da silver. Cidade com menos de K clientes
# MAGIC   não aparece com nome: a receita dela entra numa linha só, junto com a dos pedidos sem cliente (como o 106,
# MAGIC   da cliente que pediu a eliminação). Assim a soma das linhas é a receita total, e ninguém descobre a
# MAGIC   receita de uma cidade suprimida subtraindo as outras do total.

# COMMAND ----------

SUPRIMIDAS = "(outras: cidades com menos de K clientes e pedidos sem cliente)"


def medir_k(silver: str, quase_identificadores=("cidade", "faixa_etaria")):
    cols = ", ".join(quase_identificadores)
    grupos = spark.sql(f"SELECT {cols}, COUNT(*) AS pessoas FROM {silver}.clientes GROUP BY {cols} "
                       f"ORDER BY pessoas, {cols}").collect()
    k = min((g.pessoas for g in grupos), default=0)
    unicas = sum(1 for g in grupos if g.pessoas == 1)
    return k, unicas, grupos


def sql_receita_por_cidade(silver: str, k: int) -> str:
    return f"""
    WITH clientes AS (
      SELECT cliente_ps, cidade FROM {silver}.clientes
    ), por_cidade AS (
      SELECT cidade, COUNT(*) AS clientes FROM clientes GROUP BY cidade
    ), pagos AS (
      SELECT c.cidade, p.valor_centavos
      FROM {silver}.pedidos p LEFT JOIN clientes c ON p.cliente_ps = c.cliente_ps
      WHERE p.status = 'pago'
    ), receita AS (
      SELECT cidade, SUM(valor_centavos) AS centavos FROM pagos GROUP BY cidade
    ), juntas AS (
      SELECT COALESCE(pc.cidade, r.cidade) AS cidade, COALESCE(pc.clientes, 0) AS clientes,
             COALESCE(r.centavos, 0) AS centavos
      FROM por_cidade pc FULL OUTER JOIN receita r ON pc.cidade <=> r.cidade
    )
    , marcadas AS (
      SELECT CASE WHEN cidade IS NOT NULL AND clientes >= {int(k)} THEN cidade ELSE '{SUPRIMIDAS}' END AS cidade_visivel,
             CASE WHEN cidade IS NOT NULL AND clientes >= {int(k)} THEN clientes END AS clientes_visiveis,
             centavos
      FROM juntas
    )
    SELECT cidade_visivel AS cidade, clientes_visiveis AS clientes,
           CAST(SUM(centavos) / 100.0 AS DECIMAL(12, 2)) AS receita_paga
    FROM marcadas
    GROUP BY cidade_visivel, clientes_visiveis
    ORDER BY cidade_visivel = '{SUPRIMIDAS}', receita_paga DESC
    """
