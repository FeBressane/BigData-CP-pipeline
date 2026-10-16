# Databricks notebook source
# MAGIC %md
# MAGIC # Missão 9 · Funções de resiliência
# MAGIC
# MAGIC Usadas por `09a_copia_dia1`, `09b_reconstruir`, `09c_ensaio_desastre` e `09d_verificar_missao9`.
# MAGIC
# MAGIC * `criar_copia_dia1`: cópia da bronze no estado do dia 1, em dois lugares do Databricks (tabela Delta que não
# MAGIC   aceita alteração e arquivos Parquet num volume), com o offset do log e do tópico guardados junto.
# MAGIC * `restaurar_bronze`, `reaplicar_log`, `reaplicar_topico`, `carregar_lote_dia3`: os passos da recuperação.
# MAGIC * `reconstruir`: apaga a silver e a visão de consumo e recria do zero a partir da bronze.
# MAGIC
# MAGIC Quem chama precisa ter rodado antes `%run ../seguranca/06a_protecao` e `%run ../privacidade/08_esquecer_comum`.

# COMMAND ----------

import os
import json
import time
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.window import Window

RAIZ = os.path.abspath(os.path.join(os.getcwd(), ".."))
TABELAS_DIA1 = {"clientes": "clientes.csv", "pedidos": "pedidos.csv", "navegacao": "navegacao.csv"}
COLUNAS_NAV = ["evento_id", "cliente_id", "ip", "pagina", "quando"]


def nomes(catalogo):
    return {s: f"{catalogo}.lab_{s}" for s in ("origem", "log", "bronze", "silver", "gold", "controle", "copias")}


def _csv(caminho):
    return pd.read_csv(caminho, dtype=str, keep_default_na=False)


def criar_copia_dia1(catalogo: str) -> dict:
    """Cópia da bronze no estado do dia 1. A fonte é a entrada do dia 1 (dados/dia1), com o esquema da bronze."""
    N = nomes(catalogo)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {N['copias']} COMMENT 'Cópias de segurança (Missão 9). Só o pipeline lê.'")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {N['copias']}.cofre COMMENT 'Cópia em arquivo (Parquet), outro formato'")
    linhas = {}
    for t, arquivo in TABELAS_DIA1.items():
        esquema = spark.table(f"{N['origem']}.{t}").schema
        spark.createDataFrame(_csv(os.path.join(RAIZ, "dados", "dia1", arquivo))).createOrReplaceTempView("_csv")
        cols = ", ".join(f"CAST(NULLIF(`{c.name}`, '') AS {c.dataType.simpleString()}) AS `{c.name}`" for c in esquema)
        destino = f"{N['copias']}.bronze_dia1_{t}"
        spark.sql(f"DROP TABLE IF EXISTS {destino}")
        spark.sql(f"""CREATE TABLE {destino}
                      COMMENT 'Bronze no estado do dia 1 (cópia de segurança). Não aceita UPDATE nem DELETE.'
                      TBLPROPERTIES ('delta.appendOnly' = 'true')
                      AS SELECT {cols}, TIMESTAMP '2026-10-01 23:59:59' AS _carregado_em, CAST(0 AS INT) AS _execucao
                      FROM _csv""")
        spark.table(destino).write.mode("overwrite").parquet(f"/Volumes/{catalogo}/lab_copias/cofre/dia1/{t}")
        linhas[t] = spark.table(destino).count()
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {N['copias']}.metadados (
      copia STRING, criada_em TIMESTAMP, offset_log_mudancas BIGINT, offset_topico BIGINT, linhas STRING, onde STRING
    ) COMMENT 'Cada cópia com o ponto do log e do tópico em que ela foi tirada'""")
    spark.sql(f"DELETE FROM {N['copias']}.metadados WHERE copia = 'bronze_dia1'")
    # o dia 1 é anterior a qualquer evento do log de mudanças (seq começa em 1) e do tópico (seq começa em 1)
    spark.sql(f"""INSERT INTO {N['copias']}.metadados VALUES ('bronze_dia1', current_timestamp(), 0, 0,
                  '{json.dumps(linhas)}', 'lab_copias.bronze_dia1_* (Delta, appendOnly) e /Volumes/{catalogo}/lab_copias/cofre/dia1 (Parquet)')""")
    return linhas


def offsets_da_copia(catalogo: str):
    r = spark.sql(f"SELECT * FROM {nomes(catalogo)['copias']}.metadados WHERE copia = 'bronze_dia1'").first()
    return r.offset_log_mudancas, r.offset_topico


def restaurar_bronze(catalogo: str) -> dict:
    """Recria clientes, pedidos e navegacao da bronze a partir da cópia (Delta; se faltar, do Parquet do volume)."""
    N, feito = nomes(catalogo), {}
    for t in TABELAS_DIA1:
        destino, origem = f"{N['bronze']}.{t}", f"{N['copias']}.bronze_dia1_{t}"
        spark.sql(f"DROP TABLE IF EXISTS {destino}")
        if spark.catalog.tableExists(origem):
            spark.sql(f"CREATE TABLE {destino} AS SELECT * FROM {origem}")
            feito[t] = "da tabela de cópia"
        else:
            spark.read.parquet(f"/Volumes/{catalogo}/lab_copias/cofre/dia1/{t}").write.saveAsTable(destino)
            feito[t] = "do Parquet do volume"
    return feito


def reaplicar_log(catalogo: str, offset: int) -> int:
    """Aplica o log de mudanças a partir do offset da cópia. Para cada chave vale o último evento. A exclusão usa a
    chave do evento: depois do esquecer, o evento de exclusão não tem mais os dados da pessoa."""
    N = nomes(catalogo)
    eventos = spark.table(f"{N['log']}.mudancas").where(F.col("seq") > offset)
    for t in ("clientes", "pedidos"):
        esquema = spark.table(f"{N['origem']}.{t}").schema
        colunas = [c.name for c in esquema]
        ultimos = (eventos.where(F.col("tabela") == t)
                   .withColumn("_n", F.row_number().over(Window.partitionBy("chave").orderBy(F.col("seq").desc())))
                   .where("_n = 1")
                   .select("seq", "operacao", F.col("chave").alias("_chave"), F.from_json("dados", esquema).alias("d"))
                   .select("seq", "operacao", "_chave", "d.*"))
        if ultimos.isEmpty():
            continue
        ultimos.createOrReplaceTempView("_ultimos")
        atualiza = ", ".join(f"b.{c} = n.{c}" for c in colunas)
        spark.sql(f"""
          MERGE INTO {N['bronze']}.{t} b
          USING (SELECT *, current_timestamp() AS _carregado_em, 0 AS _execucao FROM _ultimos) n
          ON b.id = n._chave
          WHEN MATCHED AND n.operacao = 'exclusao' THEN DELETE
          WHEN MATCHED THEN UPDATE SET {atualiza}, b._carregado_em = n._carregado_em
          WHEN NOT MATCHED AND n.operacao <> 'exclusao' THEN
            INSERT ({", ".join(colunas)}, _carregado_em, _execucao)
            VALUES ({", ".join("n." + c for c in colunas)}, n._carregado_em, n._execucao)""")
    return eventos.count()


def reaplicar_topico(catalogo: str, offset: int) -> int:
    N = nomes(catalogo)
    novos = spark.table(f"{N['log']}.navegacao").where(F.col("seq") > offset)
    novos.createOrReplaceTempView("_topico")
    spark.sql(f"""
      MERGE INTO {N['bronze']}.navegacao b
      USING (SELECT {", ".join(COLUNAS_NAV)}, current_timestamp() AS _carregado_em, 0 AS _execucao FROM _topico) n
      ON b.evento_id = n.evento_id
      WHEN NOT MATCHED THEN INSERT ({", ".join(COLUNAS_NAV)}, _carregado_em, _execucao)
        VALUES ({", ".join("n." + c for c in COLUNAS_NAV)}, n._carregado_em, n._execucao)""")
    return novos.count()


def carregar_lote_dia3(catalogo: str) -> int:
    """O mesmo que ingestao/04a_carregar_lote_dia3: o arquivo entra na bronze como chegou."""
    N, arquivo = nomes(catalogo), "pedidos_dia3.csv"
    pdf = _csv(os.path.join(RAIZ, "dados", "dia3", arquivo))
    pdf.insert(0, "_linha", range(1, len(pdf) + 1))
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {N['bronze']}.pedidos_lote (
      id STRING, cliente_id STRING, valor_centavos STRING, status STRING, criado_em STRING, observacao STRING,
      _arquivo STRING, _linha INT, _carregado_em TIMESTAMP, _execucao INT
    ) COMMENT 'Lotes de pedidos recebidos por arquivo, como chegaram (tudo como texto)'""")
    spark.createDataFrame(pdf.astype({"_linha": int})).createOrReplaceTempView("_lote")
    spark.sql(f"""
      MERGE INTO {N['bronze']}.pedidos_lote b
      USING (SELECT id, cliente_id, valor_centavos, status, criado_em, observacao, '{arquivo}' AS _arquivo,
                    CAST(_linha AS INT) AS _linha, current_timestamp() AS _carregado_em, 1 AS _execucao FROM _lote) n
      ON b._arquivo = n._arquivo AND b._linha = n._linha
      WHEN NOT MATCHED THEN INSERT *""")
    return spark.table(f"{N['bronze']}.pedidos_lote").count()


def rodar_notebook(caminho: str, catalogo: str):
    """Roda outro notebook do repositório, com os widgets padrão dele."""
    return dbutils.notebook.run(caminho, 1800, {"catalogo": catalogo})


def reconstruir(catalogo: str) -> dict:
    """Apaga a silver e a visão de consumo e recria do zero a partir da bronze. Devolve o tempo de cada parte."""
    N, tempos = nomes(catalogo), {}
    t0 = time.perf_counter()
    for t in ("clientes", "pedidos", "navegacao"):
        spark.sql(f"DROP TABLE IF EXISTS {N['silver']}.{t}")
    spark.sql(f"DROP VIEW IF EXISTS {N['gold']}.receita_por_cidade")
    tempos["apagar silver e gold"] = time.perf_counter() - t0
    t0 = time.perf_counter()
    rodar_notebook("../qualidade/04b_validar", catalogo)
    tempos["validar e proteger (silver)"] = time.perf_counter() - t0
    t0 = time.perf_counter()
    rodar_notebook("../privacidade/07a_visao_consumo", catalogo)
    tempos["visão de consumo (gold)"] = time.perf_counter() - t0
    return tempos


def numeros(catalogo: str) -> dict:
    N = nomes(catalogo)
    r = spark.sql(f"""SELECT
      (SELECT COUNT(*) FROM {N['silver']}.clientes) AS clientes,
      (SELECT COUNT(*) FROM {N['silver']}.pedidos) AS pedidos,
      (SELECT COUNT(*) FROM {N['silver']}.navegacao) AS eventos,
      (SELECT CAST(SUM(valor_centavos) / 100.0 AS DECIMAL(12,2)) FROM {N['silver']}.pedidos WHERE status = 'pago') AS receita
    """).first()
    visao = sorted((x.cidade, x.clientes, str(x.receita_paga)) for x in spark.table(f"{N['gold']}.receita_por_cidade").collect())
    return {"clientes": r.clientes, "pedidos": r.pedidos, "eventos": r.eventos, "receita": str(r.receita), "visao": visao}
