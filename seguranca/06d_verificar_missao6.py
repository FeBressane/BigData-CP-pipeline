# Databricks notebook source
# MAGIC %md
# MAGIC # Missão 6 · Verificação e evidência
# MAGIC
# MAGIC Confere as verificações do laboratório e grava `evidencias/missao6.txt`:
# MAGIC 1. Busca por `@`, CPFs, telefones e IPs na silver (inclusive nas versões antigas, por time travel): nenhuma ocorrência.
# MAGIC 2. Mesma chave gera os mesmos pseudônimos; outra chave muda todos. O mesmo cliente tem o mesmo `cliente_ps` nas
# MAGIC    três tabelas da silver.
# MAGIC 3. O valor da chave não aparece em lugar nenhum do repositório.
# MAGIC
# MAGIC E registra o **trajeto do CPF**, medido em cada lugar onde ele pode estar, e os papéis que ficaram valendo.
# MAGIC Rodem antes `06b_criar_chave`, `qualidade/04b_validar` e `06c_papeis`. A evidência não traz dado pessoal nem a chave.

# COMMAND ----------

# MAGIC %run ../governanca/05a_governanca

# COMMAND ----------

# MAGIC %run ./06a_protecao

# COMMAND ----------

import re
import secrets as _secrets
from datetime import datetime
from pyspark.sql import functions as F

dbutils.widgets.text("catalogo", "workspace", "Catálogo")
CATALOGO = dbutils.widgets.get("catalogo")
ORIGEM, LOGS, BRONZE, SILVER, GOLD, QUALIDADE = (f"{CATALOGO}.{s}" for s in
    ("lab_origem", "lab_log", "lab_bronze", "lab_silver", "lab_gold", "lab_qualidade"))
CHAVE = obter_chave()
contratos = ler_contratos()
PADROES_BUSCA = dict(PADROES_PESSOAIS, **{"@": r"@"})


def tabelas(schema):
    try:
        return [r.tableName for r in spark.sql(f"SHOW TABLES IN {schema}").collect() if not r.isTemporary]
    except Exception:
        return []


def ocorrencias(df, padroes=PADROES_BUSCA):
    """{(coluna, padrão): linhas} para cada coluna de texto."""
    achado = {}
    for f in df.schema.fields:
        if f.dataType.simpleString() != "string":
            continue
        for nome, regex in padroes.items():
            n = df.where(F.col(f.name).rlike(regex)).count()
            if n:
                achado[(f.name, nome)] = n
    return achado

# COMMAND ----------

# 1. Busca na silver, na versão atual e em todas as versões guardadas pelo Delta
proibidas = {}
for nome, c in contratos.items():
    proibidas[nome] = {f["nome"] for f in c["campos"] if f.get("protecao") in ("remover", "pseudonimo")}
busca, versoes_vistas, colunas_proibidas = {}, 0, []
for t in tabelas(SILVER):
    historico = spark.sql(f"DESCRIBE HISTORY {SILVER}.{t}").select("version").collect()
    for v in sorted(r.version for r in historico):
        df = spark.sql(f"SELECT * FROM {SILVER}.{t} VERSION AS OF {v}")
        versoes_vistas += 1
        for (col, padrao), n in ocorrencias(df).items():
            busca[(t, v, col, padrao)] = n
        sobra = proibidas.get(t, set()) & set(df.columns)
        if sobra:
            colunas_proibidas.append(f"{t} v{v}: {', '.join(sorted(sobra))}")
colunas_atuais = {t: spark.table(f"{SILVER}.{t}").columns for t in tabelas(SILVER)}
silver_ok = (not busca and not colunas_proibidas and versoes_vistas > 0
             and all("cliente_ps" in colunas_atuais.get(t, []) for t in ("clientes", "pedidos", "navegacao")))

# COMMAND ----------

# 2. Pseudônimos
ids = [str(i) for i in range(1, 8)]
outra = _secrets.token_hex(32)
mesma_1 = [pseudonimo_de(i, CHAVE) for i in ids]
mesma_2 = [pseudonimo_de(i, CHAVE) for i in ids]
com_outra = [pseudonimo_de(i, outra) for i in ids]
del outra
deterministico = mesma_1 == mesma_2
outra_muda_todos = all(a != b for a, b in zip(mesma_1, com_outra))

# cada linha da silver tem o pseudônimo do cliente_id que veio na bronze, nas três tabelas
def confere(tabela_silver, chave_linha, fontes_bronze, coluna_cliente):
    esperado = {}
    for fonte in fontes_bronze:
        for r in spark.table(fonte).select(F.col(chave_linha).cast("string").alias("k"),
                                          F.expr(f"CAST(try_cast(`{coluna_cliente}` AS INT) AS STRING)").alias("c")).collect():
            esperado.setdefault(r.k, pseudonimo_de(r.c, CHAVE))
    silver = spark.table(f"{SILVER}.{tabela_silver}").select(
        F.col(chave_linha).cast("string").alias("k"), "cliente_ps").collect()
    certos = sum(1 for r in silver if esperado.get(r.k) == r.cliente_ps)
    return certos, len(silver)

cons = {
    "pedidos": confere("pedidos", "id", [f"{BRONZE}.pedidos", f"{BRONZE}.pedidos_lote"], "cliente_id"),
    "navegacao": confere("navegacao", "evento_id", [f"{BRONZE}.navegacao"], "cliente_id"),
}
ps_bronze_clientes = {pseudonimo_de(r.id, CHAVE) for r in spark.table(f"{BRONZE}.clientes").select(
    F.col("id").cast("string").alias("id")).collect()}
ps_silver_clientes = {r.cliente_ps for r in spark.table(f"{SILVER}.clientes").select("cliente_ps").collect()}
cons = {"clientes": (len(ps_silver_clientes & ps_bronze_clientes), len(ps_silver_clientes)), **cons}
ps1 = pseudonimo_de("1", CHAVE)
cliente1_em = [t for t in ("clientes", "pedidos", "navegacao")
               if spark.table(f"{SILVER}.{t}").where(F.col("cliente_ps") == ps1).count() > 0]
ligacao_ok = all(c == n and n > 0 for c, n in cons.values()) and len(cliente1_em) == 3
pseudonimo_ok = deterministico and outra_muda_todos and ligacao_ok

# COMMAND ----------

# 3. A chave em lugar nenhum do repositório
arquivos_lidos, com_chave = 0, []
for raiz, pastas, arquivos in os.walk(RAIZ_REPO):
    pastas[:] = [p for p in pastas if p not in (".git", "__pycache__")]
    for a in arquivos:
        caminho = os.path.join(raiz, a)
        try:
            with open(caminho, "rb") as f:
                conteudo = f.read()
        except Exception:
            continue
        arquivos_lidos += 1
        if CHAVE.encode() in conteudo:
            com_chave.append(os.path.relpath(caminho, RAIZ_REPO))
origem_chave = onde_esta_a_chave()[0]
chave_ok = arquivos_lidos > 0 and not com_chave
del CHAVE

# COMMAND ----------

# Trajeto do CPF: medido em cada lugar
CPF = PADROES_PESSOAIS["CPF"]
def linhas_com_cpf(tabela, coluna, filtro="TRUE"):
    try:
        return spark.table(tabela).where(filtro).where(F.col(coluna).rlike(CPF)).count()
    except Exception:
        return None

def estado(n_real, existe=True, mascarado=0):
    if not existe:
        return "ausente (o lugar não existe ainda)"
    if n_real:
        return f"real ({n_real} linhas)"
    if mascarado:
        return f"máscara ({mascarado} linhas)"
    return "ausente"

quarentena_clientes = spark.table(f"{QUALIDADE}.quarentena").where("conjunto = 'clientes'")
q_real = quarentena_clientes.where(F.col("dados").rlike(CPF)).count()
q_mask = quarentena_clientes.where(F.col("dados").rlike(r'"cpf":"\*\*\*')).count()
arquivos_cpf = []
for pasta in ("evidencias", "governanca", "seguranca"):
    for a in sorted(os.listdir(os.path.join(RAIZ_REPO, pasta))):
        if a.endswith((".txt", ".md")):
            texto = open(os.path.join(RAIZ_REPO, pasta, a), encoding="utf-8").read()
            # CPF ou e-mail em claro num arquivo do repositório (o IP não entra: números de versão parecem IP)
            if any(re.search(PADROES_PESSOAIS[nome], texto) for nome in ("CPF", "e-mail")):
                arquivos_cpf.append(f"{pasta}/{a}")
gold = tabelas(GOLD)
trajeto = [
    ("origem (lab_origem.clientes.cpf)", estado(linhas_com_cpf(f"{ORIGEM}.clientes", "cpf"))),
    ("log de mudanças (lab_log.mudancas.dados)", estado(linhas_com_cpf(f"{LOGS}.mudancas", "dados"))),
    ("bronze (lab_bronze.clientes.cpf)", estado(linhas_com_cpf(f"{BRONZE}.clientes", "cpf"))),
    ("quarentena (lab_qualidade.quarentena.dados)",
     estado(q_real, mascarado=q_mask) + ("" if q_real or q_mask else " · nenhum cliente reprovado; se houver, máscara")),
    ("silver (lab_silver.clientes)", "ausente (coluna não existe)" if "cpf" not in colunas_atuais.get("clientes", [])
                                      else "REAL (coluna existe)"),
    ("gold (lab_gold)", estado(0, existe=bool(gold))),
    ("varredor (lab_qualidade.varredura)", "ausente (guarda onde está, nunca o valor)"),
    ("catálogo e evidências (arquivos .md/.txt)", "REAL (CPF ou e-mail) em " + ", ".join(arquivos_cpf) if arquivos_cpf else "ausente"),
]

# COMMAND ----------

checks = [
    ("Busca por @, CPFs, telefones e IPs na silver: nenhuma ocorrência",
     silver_ok,
     f"{len(colunas_atuais)} tabelas, {versoes_vistas} versões lidas por time travel · ocorrências: {len(busca)} · "
     f"colunas proibidas: {'; '.join(colunas_proibidas) or 'nenhuma'}"),
    ("Mesma chave gera os mesmos pseudônimos; outra chave muda todos",
     pseudonimo_ok,
     f"mesma chave, duas vezes: {'iguais' if deterministico else 'DIFERENTES'} · outra chave: "
     f"{sum(a != b for a, b in zip(mesma_1, com_outra))} de {len(ids)} mudaram · "
     + " · ".join(f"{t}: {c}/{n} linhas com o pseudônimo certo" for t, (c, n) in cons.items())
     + f" · cliente 1 com o mesmo cliente_ps em: {', '.join(cliente1_em)}"),
    ("O valor da chave não aparece em lugar nenhum do repositório",
     chave_ok,
     f"{arquivos_lidos} arquivos lidos · com a chave: {', '.join(com_chave) or 'nenhum'} · "
     f"a chave fica em: {origem_chave}"),
]

linhas = [f"Missão 6 · segurança, pseudônimo e papéis · verificado em {datetime.now():%Y-%m-%d %H:%M}", ""]
for i, (desc, ok, det) in enumerate(checks, 1):
    linhas += [f"[{'OK' if ok else 'PENDENTE'}] {i}. {desc}", f"      {det}"]

linhas += ["", "Silver (colunas atuais):"]
for t, cols in sorted(colunas_atuais.items()):
    linhas.append(f"  {t:<10} {', '.join(c for c in cols if not c.startswith('_'))}")
linhas += ["", f"Pseudônimo do cliente 1 (12 primeiros caracteres): {ps1[:12]}… · HMAC-SHA-256, 64 caracteres"]

linhas += ["", "Trajeto do CPF (medido agora):"]
for lugar, st in trajeto:
    linhas.append(f"  {lugar:<45} {st}")

linhas += ["", "Papéis (SHOW GRANTS por schema):"]
for s in ("lab_bronze", "lab_log", "lab_origem", "lab_qualidade", "lab_silver", "lab_gold"):
    try:
        g = sorted({f"{r.Principal}: {r.ActionType}" for r in spark.sql(f"SHOW GRANTS ON SCHEMA {CATALOGO}.{s}").collect()})
    except Exception as e:
        g = ["(erro ao ler)"]
    linhas.append(f"  {s:<14} {'; '.join(g) or 'só o dono'}")

texto = "\n".join(linhas)
print(texto)
caminho = os.path.join(RAIZ_REPO, "evidencias", "missao6.txt")
with open(caminho, "w", encoding="utf-8") as f:
    f.write(texto + "\n")
print("\nEvidência gravada em", caminho)
