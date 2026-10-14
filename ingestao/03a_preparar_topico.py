# Databricks notebook source
# MAGIC %md
# MAGIC # Missão 3 · Preparar o tópico de navegação
# MAGIC
# MAGIC Deixa tudo no ponto de partida do exercício:
# MAGIC 1. cria o tópico `lab_log.navegacao` vazio, ao lado do `lab_log.mudancas` da Missão 2;
# MAGIC 2. apaga os offsets dos três leitores (`visitas`, `bronze` e `por_cliente` ainda não existem);
# MAGIC 3. cria vazias as saídas dos leitores, no schema `lab_leitores`;
# MAGIC 4. esvazia `lab_bronze.navegacao`: a partir de agora ela se enche lendo o tópico, e a carga em lote
# MAGIC    da Missão 1 sai de cena para a navegação.
# MAGIC
# MAGIC **Roteiro completo** (detalhado no README):
# MAGIC `03a_preparar_topico` → abrir `03c_leitor_visitas` (com "Cair no evento" = 4) e `03d_leitor_bronze`
# MAGIC em abas separadas e rodar os dois → rodar `03b_produtor` → quando o produtor terminar, religar o
# MAGIC `03c_leitor_visitas` com "Cair no evento" = 0 → `03e_leitor_por_cliente` → `03f_verificar_missao3`.

# COMMAND ----------

# MAGIC %run ./03_comum

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {LEITORES} COMMENT 'Saídas dos leitores do tópico de navegação'")

spark.sql(f"DROP TABLE IF EXISTS {TOPICO}")
spark.sql(f"""
CREATE TABLE {TOPICO} (
  seq          BIGINT    COMMENT 'Posição no tópico',
  chave        INT       COMMENT 'Chave do evento = cliente_id (mantém a ordem por cliente)',
  evento_id    INT,
  cliente_id   INT,
  ip           STRING,
  pagina       STRING,
  quando       TIMESTAMP,
  publicado_em TIMESTAMP
) COMMENT 'Tópico de navegação. Só se anexa no fim; guarda por tempo, e não até ser lido.'
""")

spark.sql(f"DELETE FROM {CONTROLE}.offsets WHERE fonte = 'navegacao'")
spark.sql(f"DROP TABLE IF EXISTS {CONTROLE}.leituras")
spark.sql(f"""
CREATE TABLE {CONTROLE}.leituras (
  leitor         STRING,
  rodada         INT,
  iniciada_em    TIMESTAMP,
  terminada_em   TIMESTAMP,
  offset_inicial BIGINT,
  offset_final   BIGINT,
  eventos_lidos  BIGINT,
  repetidos      BIGINT  COMMENT 'Eventos lidos de novo após uma queda e ignorados',
  atraso_maximo  BIGINT  COMMENT 'Maior número de eventos pendentes encontrado ao acordar',
  caiu           BOOLEAN
) COMMENT 'Uma linha por rodada de cada leitor do tópico'
""")

for tabela, colunas in [("visitas_eventos", "evento_id INT, pagina STRING, contado_em TIMESTAMP"),
                        ("por_cliente_eventos", "evento_id INT, cliente_id INT, contado_em TIMESTAMP")]:
    spark.sql(f"DROP TABLE IF EXISTS {LEITORES}.{tabela}")
    spark.sql(f"CREATE TABLE {LEITORES}.{tabela} ({colunas})")

spark.sql(f"""CREATE OR REPLACE VIEW {LEITORES}.visitas_por_pagina AS
  SELECT pagina, COUNT(*) AS visitas FROM {LEITORES}.visitas_eventos GROUP BY pagina""")
spark.sql(f"""CREATE OR REPLACE VIEW {LEITORES}.eventos_por_cliente AS
  SELECT cliente_id, COUNT(*) AS eventos FROM {LEITORES}.por_cliente_eventos GROUP BY cliente_id""")

spark.sql(f"DELETE FROM {BRONZE}.navegacao")

r = spark.sql(f"""SELECT (SELECT COUNT(*) FROM {TOPICO}) AS t, (SELECT COUNT(*) FROM {BRONZE}.navegacao) AS b,
                         (SELECT COUNT(*) FROM {ORIGEM}.navegacao) AS o""").first()
print(f"Tópico com {r.t} eventos · bronze.navegacao com {r.b} · origem com {r.o} eventos para publicar")
assert (r.t, r.b, r.o) == (0, 0, 8)
