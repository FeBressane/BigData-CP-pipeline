# Databricks notebook source
# MAGIC %md
# MAGIC # Missão 5 · Varredor de dado pessoal
# MAGIC
# MAGIC O produtor declara a classe de cada campo, mas pode esquecer. O varredor não confia na declaração: procura
# MAGIC **e-mail, telefone, CPF e IP** em todas as colunas de texto da bronze, inclusive no texto livre, e também nas
# MAGIC tabelas de log (que são outra morada do mesmo dado).
# MAGIC
# MAGIC O resultado vai para `lab_qualidade.varredura`: tabela, coluna, padrão, quantas linhas e quais chaves. O valor
# MAGIC encontrado **não** é copiado para lá, para o varredor não virar mais uma morada do dado pessoal.
# MAGIC
# MAGIC Cada achado é comparado com a classe que o contrato declara para a coluna:
# MAGIC * **declarado**: o contrato já diz que a coluna é pessoal;
# MAGIC * **achado pelo varredor**: o produtor não tinha declarado; o contrato foi corrigido e marca a coluna;
# MAGIC * **NÃO DECLARADO**: a coluna tem dado pessoal e o contrato diz outra coisa. Corrijam o contrato.

# COMMAND ----------

# MAGIC %run ./05a_governanca

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalogo", "workspace", "Catálogo")
CATALOGO = dbutils.widgets.get("catalogo")
QUALIDADE = f"{CATALOGO}.lab_qualidade"
SCHEMAS = ["lab_bronze", "lab_log"]

# Padrões de dado pessoal: PADROES_PESSOAIS, definidos em 05a_governanca (também usados na Missão 6)
PADROES = PADROES_PESSOAIS
CHAVES = ["evento_id", "id", "seq", "_linha"]                 # primeira que existir identifica a linha
CONJUNTO_DA_TABELA = {"pedidos": "pedidos", "pedidos_lote": "pedidos", "clientes": "clientes",
                      "navegacao": "navegacao"}

contratos = ler_contratos()


def classe_declarada(schema, tabela, coluna):
    if schema != "lab_bronze" or tabela not in CONJUNTO_DA_TABELA:
        return None, False
    campos = {c["nome"]: c for c in contratos[CONJUNTO_DA_TABELA[tabela]]["campos"]}
    c = campos.get(coluna, {})
    return c.get("classe"), bool(c.get("classe_encontrada_pelo_varredor"))


achados = []
for schema in SCHEMAS:
    try:
        tabelas = spark.sql(f"SHOW TABLES IN {CATALOGO}.{schema}").collect()
    except Exception:
        continue                                              # schema ainda não existe
    for t in tabelas:
        if t.isTemporary:
            continue
        nome = f"{CATALOGO}.{schema}.{t.tableName}"
        df = spark.table(nome)
        chave = next((c for c in CHAVES if c in df.columns), None)
        textos = [f.name for f in df.schema.fields if f.dataType.simpleString() == "string" and not f.name.startswith("_")]
        for col in textos:
            for padrao, regex in PADROES.items():
                linhas = df.where(F.col(col).rlike(regex))
                if chave:
                    chaves = sorted(r[0] for r in linhas.select(F.col(chave).cast("string")).distinct().collect())
                    chaves = sorted(chaves, key=lambda s: (len(s), s))
                else:
                    chaves = []
                n = linhas.count()
                if n == 0:
                    continue
                classe, pelo_varredor = classe_declarada(schema, t.tableName, col)
                if classe is None:
                    situacao = "fora do contrato (outra morada do dado)"
                elif classe in CLASSES_COM_RETENCAO:
                    situacao = "achado pelo varredor (contrato corrigido)" if pelo_varredor else "declarado"
                else:
                    situacao = f"NÃO DECLARADO: contrato diz '{classe}'"
                achados.append((schema, t.tableName, col, padrao, n, chave, ", ".join(chaves), classe, situacao))

colunas = "schema STRING, tabela STRING, coluna STRING, padrao STRING, linhas BIGINT, chave STRING, " \
          "chaves STRING, classe_declarada STRING, situacao STRING"
resultado = spark.createDataFrame(achados, colunas).withColumn("varrido_em", F.current_timestamp())
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {QUALIDADE}")
resultado.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{QUALIDADE}.varredura")
spark.sql(f"COMMENT ON TABLE {QUALIDADE}.varredura IS "
          "'Saída do varredor de dado pessoal: onde há e-mail, telefone, CPF ou IP. Não guarda o valor achado.'")

display(spark.table(f"{QUALIDADE}.varredura").orderBy("schema", "tabela", "coluna", "padrao"))

nao_declarados = [a for a in achados if a[8].startswith("NÃO DECLARADO")]
print(f"{len(achados)} achados · {len(nao_declarados)} coluna(s) com dado pessoal não declarado no contrato")
for a in nao_declarados:
    print(f"  ⚠ {a[0]}.{a[1]}.{a[2]}: {a[3]} em {a[4]} linha(s) ({a[5]} {a[6]}) · {a[8]}")
