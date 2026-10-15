# Databricks notebook source
# MAGIC %md
# MAGIC # Missão 4 · Validação pelo contrato
# MAGIC
# MAGIC O validador lê o contrato de cada conjunto em `contratos/<conjunto>.yaml` e decide, **linha por linha**,
# MAGIC o que segue da bronze para a silver. As regras vêm do arquivo; o código só sabe interpretar os tipos de
# MAGIC regra (`tipo`, `obrigatorio`, `unico`, `minimo`, `maximo`, `dominio`).
# MAGIC
# MAGIC * Linha aprovada vai para `lab_silver.<conjunto>`.
# MAGIC * Linha reprovada vai para `lab_qualidade.quarentena` com **o motivo** e **a execução em que chegou**.
# MAGIC   Ela não é apagada nem consertada em silêncio, e o resto do lote segue.
# MAGIC * Rodar de novo dá o mesmo resultado: a silver é recalculada a partir da bronze, e uma linha já em
# MAGIC   quarentena não entra duas vezes.
# MAGIC
# MAGIC | Conjunto | Fontes na bronze |
# MAGIC |---|---|
# MAGIC | pedidos | `lab_bronze.pedidos` (pelo log) + `lab_bronze.pedidos_lote` (arquivos) |
# MAGIC | clientes | `lab_bronze.clientes` (pelo log) |
# MAGIC | navegacao | `lab_bronze.navegacao` (pelo tópico) |
# MAGIC
# MAGIC **Missão 5:** antes de validar, a regra como código (`governanca/05a_governanca`) confere o acordo de
# MAGIC governança. Conjunto pessoal sem prazo de retenção, campo sem classe ou dono que não é um time param o
# MAGIC pipeline aqui, com a mensagem do que falta.
# MAGIC
# MAGIC **Missão 6:** a silver sai protegida, campo a campo, como o contrato manda (`protecao`): o id do cliente vira o
# MAGIC pseudônimo `cliente_ps` (HMAC-SHA-256 com a `CHAVE_PSEUDONIMO`, que fica fora do repositório), e nome, e-mail,
# MAGIC CPF, observação e IP não entram. Na quarentena esses campos aparecem mascarados.

# COMMAND ----------

# MAGIC %run ../governanca/05a_governanca

# COMMAND ----------

# MAGIC %run ../seguranca/06a_protecao

# COMMAND ----------

dbutils.widgets.text("catalogo", "workspace", "Catálogo")
dbutils.widgets.text("conjuntos", "pedidos,clientes,navegacao", "Conjuntos a validar")
CATALOGO = dbutils.widgets.get("catalogo")
CONJUNTOS = [c.strip() for c in dbutils.widgets.get("conjuntos").split(",") if c.strip()]
BRONZE, SILVER, QUALIDADE, CONTROLE = (f"{CATALOGO}.lab_bronze", f"{CATALOGO}.lab_silver",
                                       f"{CATALOGO}.lab_qualidade", f"{CATALOGO}.lab_controle")

# fonte: (tabela da bronze, nome da origem, coluna que identifica a linha, ordem de chegada)
FONTES = {
    "pedidos": [(f"{BRONZE}.pedidos", "log de mudanças", "id", 0),
                (f"{BRONZE}.pedidos_lote", "pedidos_dia3.csv", "_linha", 1)],
    "clientes": [(f"{BRONZE}.clientes", "log de mudanças", "id", 0)],
    "navegacao": [(f"{BRONZE}.navegacao", "tópico navegacao", "evento_id", 0)],
}
TIPOS = {"inteiro": "INT", "texto": "STRING", "data": "DATE", "data_hora": "TIMESTAMP"}

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {QUALIDADE} COMMENT 'Quarentena: o que não passou na validação, com o motivo'")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {QUALIDADE}.quarentena (
  conjunto STRING, origem STRING, linha STRING COMMENT 'Identificação da linha na origem (id ou nº da linha do arquivo)',
  chave STRING, motivo STRING, dados STRING COMMENT 'A linha como chegou, em JSON',
  execucao INT COMMENT 'Execução da validação em que a linha chegou à quarentena', quarentenado_em TIMESTAMP
) COMMENT 'Linhas reprovadas pelo contrato. Nada é apagado nem consertado em silêncio.'
""")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CONTROLE}.validacoes (
  execucao INT, conjunto STRING, versao_contrato INT, regras INT, lidas BIGINT, aceitas BIGINT,
  quarentena BIGINT, validado_em TIMESTAMP
) COMMENT 'Uma linha por conjunto em cada execução da validação'
""")
EXECUCAO = spark.sql(f"SELECT COALESCE(MAX(execucao), 0) + 1 AS n FROM {CONTROLE}.validacoes").first().n
print(f"Validação · execução {EXECUCAO} · conjuntos {', '.join(CONJUNTOS)}")
print(f"Pseudônimo: chave lida de {registrar_pseudonimo()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ler o contrato e montar as regras

# COMMAND ----------

import os
try:
    import yaml
except ImportError:
    raise ImportError("O pacote PyYAML não está instalado nesta computação. Rodem numa célula: %pip install pyyaml")


def ler_contrato(conjunto: str) -> dict:
    caminho = os.path.abspath(os.path.join(os.getcwd(), "..", "contratos", f"{conjunto}.yaml"))
    with open(caminho, encoding="utf-8") as f:
        return yaml.safe_load(f)


def literal(v) -> str:
    return "'" + str(v).replace("\\", "\\\\").replace("'", "\\'") + "'"


def expr_silver(campo: dict):
    """Como o campo entra na silver, pela proteção do contrato (None = não entra)."""
    nome, tipado = campo["nome"], f"try_cast(`{campo['nome']}` AS {TIPOS[campo['tipo']]})"
    protecao = campo.get("protecao", "manter")
    if protecao == "remover":
        return None
    if protecao == "pseudonimo":
        return f"pseudonimo(CAST({tipado} AS STRING)) AS cliente_ps"
    return f"{tipado} AS `{nome}`"


def expr_quarentena(campo: dict) -> str:
    """Como o campo aparece na quarentena: o dado pessoal nunca em claro."""
    nome, protecao = campo["nome"], campo.get("protecao", "manter")
    if protecao == "pseudonimo":
        return f"pseudonimo(CAST(try_cast(`{nome}` AS {TIPOS[campo['tipo']]}) AS STRING))"
    if protecao == "remover":
        return sql_mascara(f"`{nome}`")
    return f"`{nome}`"


def regras_sql(contrato: dict):
    """Para cada regra do contrato, uma expressão SQL que devolve o motivo da reprovação ou NULL."""
    regras = []
    for campo in contrato["campos"]:
        nome, tipo = campo["nome"], TIPOS[campo["tipo"]]
        bruto, tipado = f"c.`{nome}`", f"try_cast(c.`{nome}` AS {tipo})"
        vazio = f"({bruto} IS NULL OR trim({bruto}) = '')"
        if campo.get("obrigatorio"):
            regras.append(f"CASE WHEN {vazio} THEN '{nome} obrigatório ausente' END")
        regras.append(f"CASE WHEN NOT {vazio} AND {tipado} IS NULL THEN '{nome} não é do tipo {campo['tipo']}' END")
        if "minimo" in campo:
            regras.append(f"CASE WHEN {tipado} < {campo['minimo']} THEN '{nome} abaixo do mínimo {campo['minimo']}' END")
        if campo.get("maximo") == "momento da carga":
            regras.append(f"CASE WHEN {tipado} > current_timestamp() THEN '{nome} no futuro (depois do momento da carga)' END")
        if "dominio" in campo:
            valores = ", ".join(literal(v) for v in campo["dominio"])
            dominio = "[" + ", ".join(map(str, campo["dominio"])) + "]"
            regras.append(f"CASE WHEN NOT {vazio} AND {bruto} NOT IN ({valores}) "
                          f"THEN '{nome} fora do domínio {dominio}' END")
        if campo.get("unico"):
            regras.append(f"CASE WHEN NOT {vazio} AND ROW_NUMBER() OVER (PARTITION BY {bruto} ORDER BY c._ordem, c._pos) > 1 "
                          f"THEN '{nome} repetido (chave já recebida)' END")
    return regras

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validar cada conjunto

# COMMAND ----------

resumo = []
for conjunto in CONJUNTOS:
    contrato = ler_contrato(conjunto)
    campos = [c["nome"] for c in contrato["campos"]]
    chave = next(c["nome"] for c in contrato["campos"] if c.get("unico"))
    regras = regras_sql(contrato)

    # todas as fontes do conjunto, com as colunas do contrato como texto, na ordem de chegada
    partes = []
    for tabela, origem, id_linha, ordem in FONTES[conjunto]:
        cols = ", ".join(f"CAST(`{c}` AS STRING) AS `{c}`" for c in campos)
        partes.append(f"SELECT {cols}, '{origem}' AS _origem, CAST({id_linha} AS STRING) AS _linha, "
                      f"{ordem} AS _ordem, CAST({id_linha} AS BIGINT) AS _pos FROM {tabela}")
    spark.sql(" UNION ALL ".join(partes)).createOrReplaceTempView("_candidatas")

    spark.sql(f"""
      SELECT c.*, concat_ws('; ', {", ".join(regras)}) AS _motivo
      FROM _candidatas c
    """).createOrReplaceTempView("_avaliadas")

    # aprovadas: silver recalculada a partir da bronze (idempotente), já protegida
    tipadas = ", ".join(e for e in (expr_silver(c) for c in contrato["campos"]) if e)
    # Se a silver existente ainda tem colunas que agora são protegidas (versão sem proteção), ela é apagada antes:
    # assim as versões antigas, com dado pessoal, não ficam acessíveis por time travel na tabela nova.
    protegidos = {c["nome"] for c in contrato["campos"] if c.get("protecao") in ("remover", "pseudonimo")}
    if spark.catalog.tableExists(f"{SILVER}.{conjunto}") and protegidos & set(spark.table(f"{SILVER}.{conjunto}").columns):
        spark.sql(f"DROP TABLE {SILVER}.{conjunto}")
        print(f"{conjunto}: silver antiga, sem proteção, apagada")
    spark.sql(f"""
      CREATE OR REPLACE TABLE {SILVER}.{conjunto}
      COMMENT 'Linhas de {conjunto} aprovadas pelo contrato versão {contrato["versao"]}, protegidas (sem dado pessoal em claro)'
      AS SELECT {tipadas}, _origem, current_timestamp() AS _validado_em, {EXECUCAO} AS _execucao
      FROM _avaliadas WHERE _motivo = ''
    """)

    # reprovadas: quarentena com o motivo; a execução registrada é a da primeira vez que a linha chegou.
    # O dado pessoal entra mascarado ou como pseudônimo (rodar de novo também limpa as linhas antigas).
    dados_q = ", ".join(f"{expr_quarentena(c)} AS `{c['nome']}`" for c in contrato["campos"])
    chave_q = expr_quarentena(next(c for c in contrato["campos"] if c["nome"] == chave))
    spark.sql(f"""
      MERGE INTO {QUALIDADE}.quarentena q
      USING (SELECT '{conjunto}' AS conjunto, _origem AS origem, _linha AS linha, {chave_q} AS chave,
                    _motivo AS motivo, to_json(struct({dados_q})) AS dados
             FROM _avaliadas WHERE _motivo <> '') n
      ON q.conjunto = n.conjunto AND q.origem = n.origem AND q.linha = n.linha
      WHEN MATCHED THEN UPDATE SET q.motivo = n.motivo, q.dados = n.dados
      WHEN NOT MATCHED THEN INSERT (conjunto, origem, linha, chave, motivo, dados, execucao, quarentenado_em)
        VALUES (n.conjunto, n.origem, n.linha, n.chave, n.motivo, n.dados, {EXECUCAO}, current_timestamp())
    """)

    r = spark.sql("SELECT COUNT(*) AS lidas, SUM(CASE WHEN _motivo = '' THEN 1 ELSE 0 END) AS aceitas FROM _avaliadas").first()
    quarentena = r.lidas - r.aceitas
    spark.sql(f"""INSERT INTO {CONTROLE}.validacoes VALUES ({EXECUCAO}, '{conjunto}', {contrato['versao']}, {len(regras)},
                  {r.lidas}, {r.aceitas}, {quarentena}, current_timestamp())""")
    resumo.append((conjunto, len(regras), r.lidas, r.aceitas, quarentena))
    print(f"{conjunto:<10} · contrato v{contrato['versao']} com {len(regras)} regras · lidas {r.lidas} · "
          f"aceitas {r.aceitas} · quarentena {quarentena}")

# COMMAND ----------

display(spark.sql(f"""SELECT conjunto, origem, linha, chave, motivo, execucao FROM {QUALIDADE}.quarentena
                      ORDER BY conjunto, origem, CAST(linha AS INT)"""))

# COMMAND ----------

if "pedidos" in CONJUNTOS:
    r = spark.sql(f"""SELECT COUNT(*) AS n, COALESCE(SUM(CASE WHEN status = 'pago' THEN valor_centavos END), 0) AS receita
                      FROM {SILVER}.pedidos""").first()
    print(f"lab_silver.pedidos: {r.n} pedidos · receita paga R$ {r.receita / 100:.2f}".replace(".", ","))
