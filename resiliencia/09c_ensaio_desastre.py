# Databricks notebook source
# MAGIC %md
# MAGIC # Missão 9 · Ensaio do desastre
# MAGIC
# MAGIC Simula a perda da bronze e mede quanto tempo leva para voltar (o **RTO real**):
# MAGIC
# MAGIC 1. **Apagar** a bronze inteira (`clientes`, `pedidos`, `navegacao`, `pedidos_lote`).
# MAGIC 2. **Restaurar** a cópia do dia 1 (`lab_copias`).
# MAGIC 3. **Reaplicar o log** de mudanças e o tópico a partir dos offsets guardados com a cópia.
# MAGIC 4. **Reaplicar a lista de eliminação**: a cópia do dia 1 tem a cliente 5, e ela não pode voltar.
# MAGIC 5. **Carregar o dia 3** (o lote de pedidos por arquivo).
# MAGIC 6. **Reconstruir** a silver e a visão de consumo.
# MAGIC
# MAGIC Cada etapa é cronometrada e gravada em `lab_controle.ensaios`. **Isto apaga a bronze de verdade**: o widget
# MAGIC "Confirmo apagar a bronze" precisa estar em `sim`. Rodem antes `09a_copia_dia1`.

# COMMAND ----------

# MAGIC %run ../seguranca/06a_protecao

# COMMAND ----------

# MAGIC %run ../privacidade/08_esquecer_comum

# COMMAND ----------

# MAGIC %run ./09_resiliencia_comum

# COMMAND ----------

from datetime import datetime

dbutils.widgets.text("catalogo", "workspace", "Catálogo")
dbutils.widgets.dropdown("confirmo", "nao", ["nao", "sim"], "Confirmo apagar a bronze")
CATALOGO = dbutils.widgets.get("catalogo")
assert dbutils.widgets.get("confirmo") == "sim", "Mudem o widget 'Confirmo apagar a bronze' para sim e rodem de novo."
N = nomes(CATALOGO)
assert spark.catalog.tableExists(f"{N['copias']}.bronze_dia1_clientes"), "Rodem 09a_copia_dia1 antes do ensaio."

esperado = numeros(CATALOGO)          # o estado da Missão 8, para comparar no fim
off_log, off_topico = offsets_da_copia(CATALOGO)
INICIO = datetime.now()
tempos, detalhes = {}, {}


def etapa(nome, funcao):
    t0 = time.perf_counter()
    detalhes[nome] = funcao()
    tempos[nome] = time.perf_counter() - t0
    print(f"{nome:<34} {tempos[nome]:6.1f} s · {detalhes[nome]}")


def apagar_bronze():
    for t in ("clientes", "pedidos", "navegacao", "pedidos_lote"):
        spark.sql(f"DROP TABLE IF EXISTS {N['bronze']}.{t}")
    return "4 tabelas apagadas"


etapa("1. apagar a bronze", apagar_bronze)
etapa("2. restaurar a cópia do dia 1", lambda: restaurar_bronze(CATALOGO))
etapa("3. reaplicar log e tópico", lambda: f"{reaplicar_log(CATALOGO, off_log)} eventos do log, "
                                           f"{reaplicar_topico(CATALOGO, off_topico)} do tópico")
etapa("4. reaplicar lista de eliminação", lambda: {k: sum(v.values()) for k, v in reaplicar_lista(CATALOGO).items()})
etapa("5. carregar o dia 3", lambda: f"{carregar_lote_dia3(CATALOGO)} linhas no lote")
etapa("6. reconstruir silver e gold", lambda: {k: round(v, 1) for k, v in reconstruir(CATALOGO).items()})

# os leitores voltam a apontar para o fim do log e do tópico
spark.sql(f"""MERGE INTO {N['controle']}.offsets o
  USING (SELECT 'bronze' AS consumidor, 'mudancas' AS fonte, (SELECT COALESCE(MAX(seq), 0) FROM {N['log']}.mudancas) AS posicao
         UNION ALL
         SELECT 'bronze', 'navegacao', (SELECT COALESCE(MAX(seq), 0) FROM {N['log']}.navegacao)) n
  ON o.consumidor = n.consumidor AND o.fonte = n.fonte
  WHEN MATCHED THEN UPDATE SET o.posicao = n.posicao, o.atualizado_em = current_timestamp()
  WHEN NOT MATCHED THEN INSERT (consumidor, fonte, posicao, atualizado_em) VALUES (n.consumidor, n.fonte, n.posicao, current_timestamp())""")

total = sum(tempos.values())
depois = numeros(CATALOGO)
print(f"\nTempo total (RTO real): {total:.1f} s ({total / 60:.1f} min)")
print("Números iguais aos de antes do desastre:", "sim" if depois == esperado else "NÃO")
print("Antes: ", esperado)
print("Depois:", depois)

spark.sql(f"""CREATE TABLE IF NOT EXISTS {N['controle']}.ensaios (
  ensaio TIMESTAMP, etapa STRING, segundos DOUBLE, detalhe STRING
) COMMENT 'Ensaios de desastre: tempo de cada etapa da recuperação (Missão 9)'""")
linhas = [(INICIO, e, float(s), str(detalhes[e])[:500]) for e, s in tempos.items()]
linhas.append((INICIO, "total", float(total), "iguais" if depois == esperado else f"diferentes: {depois}"))
spark.createDataFrame(linhas, "ensaio TIMESTAMP, etapa STRING, segundos DOUBLE, detalhe STRING") \
     .write.mode("append").saveAsTable(f"{N['controle']}.ensaios")
