# Databricks notebook source
# MAGIC %md
# MAGIC # Missão 9 · Cópias de segurança
# MAGIC
# MAGIC 1. **Bronze no estado do dia 1**, em dois lugares do Databricks: tabelas `lab_copias.bronze_dia1_*` (Delta com
# MAGIC    `delta.appendOnly`: não aceitam UPDATE nem DELETE) e arquivos Parquet em `/Volumes/<catálogo>/lab_copias/cofre`.
# MAGIC 2. **Offset do log e do tópico** guardados com a cópia (`lab_copias.metadados`): o dia 1 é anterior a qualquer
# MAGIC    evento, então a recuperação reaplica o log e o tópico desde o início.
# MAGIC 3. **Cópia da chave em outro lugar:** além do secret scope `lab_bd`, um arquivo na pasta pessoal do usuário, fora
# MAGIC    do repositório. A terceira cópia, offline, é do dono: o gerenciador de senhas do time (feita à mão, nunca aqui).
# MAGIC
# MAGIC A cópia do dia 1 tem a cliente 5. É por isso que a lista de eliminação é reaplicada depois de toda restauração.

# COMMAND ----------

# MAGIC %run ../seguranca/06a_protecao

# COMMAND ----------

# MAGIC %run ../privacidade/08_esquecer_comum

# COMMAND ----------

# MAGIC %run ./09_resiliencia_comum

# COMMAND ----------

dbutils.widgets.text("catalogo", "workspace", "Catálogo")
CATALOGO = dbutils.widgets.get("catalogo")

linhas = criar_copia_dia1(CATALOGO)
print("Cópia da bronze no estado do dia 1:", ", ".join(f"{t} {n}" for t, n in linhas.items()))
print("Offsets guardados com a cópia (log de mudanças, tópico):", offsets_da_copia(CATALOGO))
display(spark.sql(f"SELECT * FROM {CATALOGO}.lab_copias.metadados"))

# COMMAND ----------

# Cópia da chave em outro lugar (arquivo na pasta pessoal, fora do Git folder). O valor nunca é impresso.
origem, chave = onde_esta_a_chave()
caminho = caminho_chave_reserva()
try:
    atual = open(caminho, encoding="utf-8").read().strip()
except Exception:
    atual = None
if atual != chave:
    os.makedirs(os.path.dirname(caminho), exist_ok=True)
    with open(caminho, "w", encoding="utf-8") as f:
        f.write(chave)
igual = open(caminho, encoding="utf-8").read().strip() == chave
del chave, atual
print(f"Chave principal: {origem}")
print(f"Cópia da chave: {caminho} · confere com a principal: {'sim' if igual else 'NÃO'}")
assert igual
