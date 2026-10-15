# Databricks notebook source
# MAGIC %md
# MAGIC # Missão 3 · Um log, vários leitores (simulação completa num notebook só)
# MAGIC
# MAGIC Um **Run all** faz o exercício inteiro, na ordem do laboratório:
# MAGIC
# MAGIC 1. prepara o tópico `lab_log.navegacao` vazio (offsets, histórico e saídas dos leitores apagados);
# MAGIC 2. o **produtor** publica os 8 eventos de navegação do dia 1, um por vez. Enquanto isso:
# MAGIC    * o leitor rápido **`visitas`** acorda a cada evento e conta as visitas por página;
# MAGIC    * a **bronze** acorda a cada 5 eventos e grava em rajada o que chegou desde o seu offset;
# MAGIC    * o `visitas` **cai depois do 4º evento**, antes de salvar o offset, e fica fora do ar;
# MAGIC 3. quando o produtor termina, a bronze alcança o fim do tópico e o `visitas` é **religado**:
# MAGIC    retoma do offset salvo, relê o 4º evento, reconhece que já foi contado e chega aos mesmos totais;
# MAGIC 4. o leitor novo **`por_cliente`** liga depois de tudo publicado e lê desde o offset 0.
# MAGIC
# MAGIC Depois, rodem `03f_verificar_missao3`.
# MAGIC
# MAGIC **Relógio lógico.** No laboratório o produtor publica 1 evento por segundo e a bronze acorda a cada
# MAGIC 5 segundos. No Databricks serverless cada escrita numa tabela Delta leva de 1 a 3 segundos, então o
# MAGIC tempo de parede se estica. Para manter a proporção do exercício, a simulação conta o tempo em
# MAGIC **ticks**: 1 tick = 1 evento publicado = 1 segundo do laboratório. A bronze acorda a cada 5 ticks.
# MAGIC
# MAGIC Os notebooks `03a` a `03e` fazem o mesmo em abas separadas, com tempo real, para quem quiser ver
# MAGIC os leitores rodando em paralelo.

# COMMAND ----------

# MAGIC %run ./03_comum

# COMMAND ----------

dbutils.widgets.dropdown("cair_no", "4", [str(i) for i in range(0, 9)], "visitas cai no evento (0 = não cai)")
dbutils.widgets.text("bronze_cada", "5", "Bronze acorda a cada (ticks)")
CAIR_NO = int(dbutils.widgets.get("cair_no"))
BRONZE_CADA = int(dbutils.widgets.get("bronze_cada"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Preparar o tópico

# COMMAND ----------

preparar_topico()
eventos = eventos_do_dia1()
print(f"[{agora()}] tópico vazio · {len(eventos)} eventos do dia 1 para publicar · "
      f"bronze.navegacao com {spark.table(f'{BRONZE}.navegacao').count()} eventos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Publicar, com os leitores acordando no seu ritmo

# COMMAND ----------

visitas = Leitor("visitas", processar_visitas)
bronze = Leitor("bronze", processar_bronze)

for tick, e in enumerate(eventos, start=1):
    print(f"--- tick {tick} ---")
    publicar(e)
    if visitas is not None:
        try:
            visitas.acordar(CAIR_NO)
        except QuedaSimulada:
            visitas.encerrar()
            visitas = None
            print(f"[{agora()}] visitas   · FORA DO AR até o produtor terminar")
    if tick % BRONZE_CADA == 0:
        bronze.acordar()

print(f"[{agora()}] produtor  · terminou · tópico com {ultimo_seq()} eventos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · A bronze alcança o fim e o `visitas` é religado

# COMMAND ----------

print("--- próximo despertar da bronze ---")
while bronze.offset < ultimo_seq():
    bronze.acordar()
bronze.encerrar()

if visitas is None:
    print("--- religando o visitas ---")
    visitas = Leitor("visitas", processar_visitas)
while visitas.offset < ultimo_seq():
    visitas.acordar()
visitas.encerrar()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · O leitor novo chega depois de tudo

# COMMAND ----------

por_cliente = Leitor("por_cliente", processar_por_cliente)
while por_cliente.offset < ultimo_seq():
    por_cliente.acordar()
por_cliente.encerrar()

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {LEITORES}.visitas_por_pagina ORDER BY visitas DESC, pagina"))
display(spark.sql(f"""SELECT COALESCE(CAST(cliente_id AS STRING), 'visitante sem cadastro') AS cliente, eventos
                      FROM {LEITORES}.eventos_por_cliente ORDER BY cliente_id NULLS LAST"""))
display(spark.sql(f"SELECT * FROM {CONTROLE}.leituras ORDER BY iniciada_em, leitor"))
