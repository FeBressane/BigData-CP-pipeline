# Databricks notebook source
# MAGIC %md
# MAGIC # Missão 1 · Carga incremental
# MAGIC
# MAGIC Copia a origem (`lab_origem`) para a camada bronze (`lab_bronze`) **sem transformar nada**.
# MAGIC
# MAGIC | Tabela | Como entra na bronze | Marca-d'água |
# MAGIC |---|---|---|
# MAGIC | `clientes` | mesclagem pela chave `id` (nova entra, existente é substituída) | maior `atualizado_em` já carregado |
# MAGIC | `pedidos` | mesclagem pela chave `id` | maior `atualizado_em` já carregado |
# MAGIC | `navegacao` | anexação; `evento_id` impede a duplicata | maior `evento_id` já carregado |
# MAGIC
# MAGIC * A marca-d'água fica **fora do código**, na tabela `lab_controle.marca_dagua`.
# MAGIC * Cada execução recebe um número (`_execucao`) e é registrada em `lab_controle.execucoes`.
# MAGIC * Rodar de novo sem mudança na origem lê 0 linhas e não muda nada (idempotência).
# MAGIC
# MAGIC **Ordem do exercício:** rodar este notebook → rodar de novo → `01b_aplicar_dia2` → rodar este de novo → `01c_verificar_missao1`.

# COMMAND ----------

dbutils.widgets.text("catalogo", "workspace", "Catálogo")
dbutils.widgets.dropdown("recomecar", "nao", ["nao", "sim"], "Apagar bronze e estado antes?")
CATALOGO = dbutils.widgets.get("catalogo")
RECOMECAR = dbutils.widgets.get("recomecar") == "sim"

ORIGEM, BRONZE, CONTROLE = (f"{CATALOGO}.lab_origem", f"{CATALOGO}.lab_bronze", f"{CATALOGO}.lab_controle")

if RECOMECAR:
    for t in [f"{BRONZE}.clientes", f"{BRONZE}.pedidos", f"{BRONZE}.navegacao",
              f"{CONTROLE}.marca_dagua", f"{CONTROLE}.execucoes"]:
        spark.sql(f"DROP TABLE IF EXISTS {t}")
    print("Bronze e estado apagados. A próxima carga começa do zero.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Estado do pipeline (fora do código)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CONTROLE}.marca_dagua (
  tabela        STRING NOT NULL,
  coluna        STRING NOT NULL,
  valor         STRING,
  atualizado_em TIMESTAMP
) COMMENT 'Marca-d\\'água de cada tabela: até onde a carga incremental já leu'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CONTROLE}.execucoes (
  execucao        INT,
  missao          STRING,
  iniciada_em     TIMESTAMP,
  lidas_clientes  BIGINT,
  lidas_pedidos   BIGINT,
  lidas_navegacao BIGINT,
  bronze_clientes BIGINT,
  bronze_pedidos  BIGINT,
  bronze_eventos  BIGINT,
  receita_paga_centavos BIGINT
) COMMENT 'Uma linha por execução da carga'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabelas da bronze
# MAGIC Mesmas colunas da origem, mais `_carregado_em` (quando chegou) e `_execucao` (em qual execução).

# COMMAND ----------

for tabela in ["clientes", "pedidos", "navegacao"]:
    spark.sql(f"""
      CREATE TABLE IF NOT EXISTS {BRONZE}.{tabela} AS
      SELECT *, CAST(NULL AS TIMESTAMP) AS _carregado_em, CAST(NULL AS INT) AS _execucao
      FROM {ORIGEM}.{tabela} WHERE 1 = 0
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## A carga

# COMMAND ----------

from datetime import datetime

EXECUCAO = spark.sql(f"SELECT COALESCE(MAX(execucao), 0) + 1 AS n FROM {CONTROLE}.execucoes").first().n
INICIO = datetime.now()
print(f"Execução {EXECUCAO} · {INICIO:%Y-%m-%d %H:%M:%S}")


def ler_marca(tabela: str):
    r = spark.sql(f"SELECT valor FROM {CONTROLE}.marca_dagua WHERE tabela = '{tabela}'").first()
    return r.valor if r else None


def gravar_marca(tabela: str, coluna: str, valor) -> None:
    if valor is None:
        return  # nada novo: a marca continua onde estava
    spark.sql(f"""
      MERGE INTO {CONTROLE}.marca_dagua m
      USING (SELECT '{tabela}' AS tabela, '{coluna}' AS coluna, '{valor}' AS valor) n
      ON m.tabela = n.tabela
      WHEN MATCHED THEN UPDATE SET m.valor = n.valor, m.atualizado_em = current_timestamp()
      WHEN NOT MATCHED THEN INSERT (tabela, coluna, valor, atualizado_em)
        VALUES (n.tabela, n.coluna, n.valor, current_timestamp())
    """)


def carregar_mesclando(tabela: str) -> int:
    """clientes e pedidos: lê só atualizado_em > marca e mescla pela chave id."""
    marca = ler_marca(tabela)
    filtro = f"WHERE atualizado_em > TIMESTAMP '{marca}'" if marca else ""
    novas = spark.sql(f"SELECT * FROM {ORIGEM}.{tabela} {filtro}")
    novas.createOrReplaceTempView("_novas")
    lidas = novas.count()
    if lidas:
        spark.sql(f"""
          MERGE INTO {BRONZE}.{tabela} b
          USING (SELECT *, current_timestamp() AS _carregado_em, {EXECUCAO} AS _execucao FROM _novas) n
          ON b.id = n.id
          WHEN MATCHED THEN UPDATE SET *
          WHEN NOT MATCHED THEN INSERT *
        """)
        nova_marca = spark.sql("SELECT CAST(MAX(atualizado_em) AS STRING) AS m FROM _novas").first().m
        gravar_marca(tabela, "atualizado_em", nova_marca)
    return lidas


def carregar_anexando_navegacao() -> int:
    """navegacao: só acrescenta. Lê evento_id > marca e insere o que ainda não existe."""
    marca = ler_marca("navegacao")
    filtro = f"WHERE evento_id > {int(marca)}" if marca else ""
    novas = spark.sql(f"SELECT * FROM {ORIGEM}.navegacao {filtro}")
    novas.createOrReplaceTempView("_novas_nav")
    lidas = novas.count()
    if lidas:
        # o MERGE só com NOT MATCHED é uma anexação que não duplica o evento_id
        spark.sql(f"""
          MERGE INTO {BRONZE}.navegacao b
          USING (SELECT *, current_timestamp() AS _carregado_em, {EXECUCAO} AS _execucao FROM _novas_nav) n
          ON b.evento_id = n.evento_id
          WHEN NOT MATCHED THEN INSERT *
        """)
        nova_marca = spark.sql("SELECT CAST(MAX(evento_id) AS STRING) AS m FROM _novas_nav").first().m
        gravar_marca("navegacao", "evento_id", nova_marca)
    return lidas


lidas = {
    "clientes": carregar_mesclando("clientes"),
    "pedidos": carregar_mesclando("pedidos"),
    "navegacao": carregar_anexando_navegacao(),
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resultado desta execução

# COMMAND ----------

r = spark.sql(f"""
SELECT
  (SELECT COUNT(*) FROM {BRONZE}.clientes)  AS clientes,
  (SELECT COUNT(*) FROM {BRONZE}.pedidos)   AS pedidos,
  (SELECT COUNT(*) FROM {BRONZE}.navegacao) AS eventos,
  (SELECT COALESCE(SUM(valor_centavos), 0) FROM {BRONZE}.pedidos WHERE status = 'pago') AS receita
""").first()

spark.sql(f"""
INSERT INTO {CONTROLE}.execucoes VALUES (
  {EXECUCAO}, 'missao1', TIMESTAMP '{INICIO:%Y-%m-%d %H:%M:%S}',
  {lidas['clientes']}, {lidas['pedidos']}, {lidas['navegacao']},
  {r.clientes}, {r.pedidos}, {r.eventos}, {r.receita}
)""")

total_lidas = sum(lidas.values())
print(f"Execução {EXECUCAO}: leu {total_lidas} linhas "
      f"(clientes {lidas['clientes']}, pedidos {lidas['pedidos']}, navegação {lidas['navegacao']})")
print(f"Bronze: {r.clientes} clientes, {r.pedidos} pedidos, {r.eventos} eventos, "
      f"receita paga R$ {r.receita / 100:.2f}".replace(".", ","))

display(spark.sql(f"SELECT * FROM {CONTROLE}.marca_dagua ORDER BY tabela"))
