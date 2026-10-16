# Databricks notebook source
# MAGIC %md
# MAGIC # Missão 9 · Verificação e evidência
# MAGIC
# MAGIC Confere as três verificações do laboratório e grava `evidencias/missao9.txt`:
# MAGIC 1. A cliente 5, presente na cópia do dia 1, não reaparece em nenhuma camada.
# MAGIC 2. Números iguais aos da Missão 8: 6 clientes, 11 pedidos, 6 eventos, R$ 844,90 e a mesma visão de consumo.
# MAGIC 3. Tempo medido no ensaio = RTO real, comparado com o RTO prometido em `resiliencia/plano.md`.
# MAGIC
# MAGIC Rodem antes `09a_copia_dia1` e `09c_ensaio_desastre`. A evidência não traz dado pessoal.

# COMMAND ----------

# MAGIC %run ../seguranca/06a_protecao

# COMMAND ----------

# MAGIC %run ../privacidade/07_consumo_comum

# COMMAND ----------

# MAGIC %run ../privacidade/08_esquecer_comum

# COMMAND ----------

# MAGIC %run ./09_resiliencia_comum

# COMMAND ----------

from datetime import datetime
from decimal import Decimal

dbutils.widgets.text("catalogo", "workspace", "Catálogo")
CATALOGO = dbutils.widgets.get("catalogo")
N = nomes(CATALOGO)
RTO_PROMETIDO_S = 15 * 60          # resiliencia/plano.md: bronze + silver + gold de volta em até 15 minutos
CAMADAS = ["lab_origem", "lab_log", "lab_bronze", "lab_silver", "lab_gold", "lab_leitores", "lab_qualidade"]
TERMOS = ["Elisa", "555.555.555-55", "(13) 90000-0005", "203.0.113.55"]
PS5 = pseudonimo_de("5", obter_chave())

# 1. A cliente 5 não volta
achados, lidas = [], 0
for schema in CAMADAS:
    try:
        tabelas = [r.tableName for r in spark.sql(f"SHOW TABLES IN {CATALOGO}.{schema}").collect() if not r.isTemporary]
    except Exception:
        continue
    for t in tabelas:
        df = spark.table(f"{CATALOGO}.{schema}.{t}")
        lidas += 1
        textos = [f.name for f in df.schema.fields if f.dataType.simpleString() == "string"]
        for i, termo in enumerate(TERMOS + [PS5], 1):
            for col in textos:
                n = df.where(F.col(col).contains(termo)).count()
                if n:
                    achados.append(f"{schema}.{t}.{col} ({'termo ' + str(i) if i <= 4 else 'pseudônimo'}: {n})")
na_copia = spark.table(f"{N['copias']}.bronze_dia1_clientes").where(F.col("nome").startswith(TERMOS[0])).count()

# 2. Números
num = numeros(CATALOGO)
visao_esperada = sorted([("Santos", 3, "545.00"), ("Sorocaba", 2, "95.90"), (SUPRIMIDAS, None, "204.00")])
numeros_ok = (num["clientes"], num["pedidos"], num["eventos"], num["receita"]) == (6, 11, 6, "844.90") \
             and num["visao"] == visao_esperada

# 3. RTO real
ensaios = spark.sql(f"SELECT * FROM {N['controle']}.ensaios WHERE ensaio = (SELECT MAX(ensaio) FROM {N['controle']}.ensaios) "
                    f"ORDER BY etapa").collect()
total = next((e for e in ensaios if e.etapa == "total"), None)
rto_ok = total is not None and total.detalhe == "iguais"

checks = [
    ("A cliente 5 (presente na cópia do dia 1) não reaparece em nenhuma camada",
     not achados and lidas > 0 and na_copia == 1,
     f"{lidas} tabelas e visões em {len(CAMADAS)} schemas · ocorrências: {'; '.join(achados) or 'nenhuma'} · "
     f"a cópia do dia 1 tem a cliente: {'sim' if na_copia else 'não'} (por isso a lista é reaplicada)"),
    ("Números iguais à Missão 8: 6 clientes, 11 pedidos, 6 eventos, R$ 844,90, mesma visão",
     numeros_ok,
     f"{num['clientes']} clientes · {num['pedidos']} pedidos · {num['eventos']} eventos · R$ {num['receita'].replace('.', ',')} · "
     f"visão {'igual' if num['visao'] == visao_esperada else 'DIFERENTE'}"),
    ("Tempo medido = RTO real; comparado com o prometido e a diferença registrada",
     rto_ok,
     (f"RTO real {total.segundos:.0f} s ({total.segundos / 60:.1f} min) · prometido {RTO_PROMETIDO_S // 60} min · "
      f"folga de {(RTO_PROMETIDO_S - total.segundos) / 60:.1f} min" if total else "nenhum ensaio registrado")),
]

linhas = [f"Missão 9 · resiliência · verificado em {datetime.now():%Y-%m-%d %H:%M}", ""]
for i, (desc, ok, det) in enumerate(checks, 1):
    linhas += [f"[{'OK' if ok else 'PENDENTE'}] {i}. {desc}", f"      {det}"]

if total:
    linhas += ["", f"Ensaio do desastre de {total.ensaio:%Y-%m-%d %H:%M} (lab_controle.ensaios):"]
    for e in ensaios:
        if e.etapa != "total":
            linhas.append(f"  {e.etapa:<34} {e.segundos:7.1f} s   {e.detalhe[:90]}")
    linhas.append(f"  {'total (RTO real)':<34} {total.segundos:7.1f} s")

off = offsets_da_copia(CATALOGO)
meta = spark.sql(f"SELECT * FROM {N['copias']}.metadados WHERE copia = 'bronze_dia1'").first()
linhas += ["", "Cópias (3-2-1-1-0, detalhe em resiliencia/plano.md):",
           f"  bronze do dia 1: {meta.linhas} · em {meta.onde}",
           f"  offsets guardados com a cópia: log de mudanças {off[0]}, tópico {off[1]}",
           f"  chave: secret scope lab_bd + arquivo na pasta pessoal ({'existe' if os.path.exists(caminho_chave_reserva()) else 'FALTA'})"
           " + cópia offline do dono (gerenciador de senhas)",
           "", "Ordem da recuperação: restaurar a cópia, reaplicar log e tópico, reaplicar a lista de eliminação,",
           "carregar o dia 3, reconstruir silver e gold. Sem a lista, a cliente 5 voltaria com a cópia do dia 1."]

texto = "\n".join(linhas)
print(texto)
caminho = os.path.join(RAIZ, "evidencias", "missao9.txt")
with open(caminho, "w", encoding="utf-8") as f:
    f.write(texto + "\n")
print("\nEvidência gravada em", caminho)
