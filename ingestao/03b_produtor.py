# Databricks notebook source
# MAGIC %md
# MAGIC # Missão 3 · Produtor
# MAGIC
# MAGIC Publica no tópico `lab_log.navegacao` os 8 eventos de navegação do dia 1, **um a cada segundo**.
# MAGIC O produtor não sabe quem vai ler: ele só anexa no fim do tópico.
# MAGIC
# MAGIC A chave de cada evento é o `cliente_id`, para manter a ordem por cliente. Publicar de novo um
# MAGIC `evento_id` que já está no tópico não duplica nada.
# MAGIC
# MAGIC Rodem este notebook **depois** de ligar o `03c_leitor_visitas` e o `03d_leitor_bronze` em outras abas.

# COMMAND ----------

# MAGIC %run ./03_comum

# COMMAND ----------

dbutils.widgets.text("intervalo", "1", "Segundos entre eventos")
INTERVALO = float(dbutils.widgets.get("intervalo"))

eventos = spark.sql(f"SELECT * FROM {ORIGEM}.navegacao ORDER BY evento_id").collect()
print(f"[{agora()}] produtor · {len(eventos)} eventos para publicar, 1 a cada {INTERVALO:g} s")

for e in eventos:
    cliente = "NULL" if e.cliente_id is None else e.cliente_id
    spark.sql(f"""
      INSERT INTO {TOPICO}
      SELECT COALESCE(MAX(seq), 0) + 1, {cliente}, {e.evento_id}, {cliente}, '{e.ip}', '{e.pagina}',
             TIMESTAMP '{e.quando:%Y-%m-%d %H:%M:%S}', current_timestamp()
      FROM {TOPICO}
      HAVING SUM(CASE WHEN evento_id = {e.evento_id} THEN 1 ELSE 0 END) = 0 OR COUNT(*) = 0
    """)
    print(f"[{agora()}] publicou evento {e.evento_id} · {e.pagina} · tópico no seq {ultimo_seq()}")
    time.sleep(INTERVALO)

print(f"[{agora()}] produtor terminou · tópico com {ultimo_seq()} eventos")
