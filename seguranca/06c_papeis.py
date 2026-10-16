# Databricks notebook source
# MAGIC %md
# MAGIC # Missão 6 · Papéis: o pipeline lê a bronze, o analista só a silver
# MAGIC
# MAGIC As permissões ficam escritas aqui, como código, no Unity Catalog:
# MAGIC
# MAGIC | Papel | Principal | Pode ler | Não pode ler |
# MAGIC |---|---|---|---|
# MAGIC | Pipeline | grupo `lab_pipeline`, ou o dono dos schemas (quem roda o pipeline) | origem, log, bronze, controle, qualidade, silver, gold | |
# MAGIC | Analista | grupo `lab_analistas`, ou `account users` (todo usuário da conta) | silver e gold | origem, log, bronze, controle, qualidade, leitores |
# MAGIC
# MAGIC **Por que há um plano B.** O Unity Catalog só aceita grupos **da conta**. Na Free Edition não existe console da
# MAGIC conta, e o grupo criado pelo workspace (`lab_pipeline`, `lab_analistas`) é local: o `GRANT` responde
# MAGIC `PRINCIPAL_DOES_NOT_EXIST`. Nesse caso o notebook usa os principais que o Unity Catalog sempre conhece:
# MAGIC
# MAGIC * **analista = `account users`**: qualquer pessoa da conta que não seja o dono só lê silver e gold. É a regra
# MAGIC   mais segura: o padrão para todo mundo é o papel de analista.
# MAGIC * **pipeline = dono dos schemas**: o usuário que roda os notebooks (e os jobs, que rodam como ele). Dono tem todas
# MAGIC   as permissões sem precisar de `GRANT`.
# MAGIC
# MAGIC Rodar de novo dá o mesmo resultado. No fim o notebook mostra as permissões que ficaram valendo, inclusive as
# MAGIC herdadas do catálogo, e **para** se o analista conseguir ler alguma camada com dado pessoal em claro.

# COMMAND ----------

dbutils.widgets.text("catalogo", "workspace", "Catálogo")
CATALOGO = dbutils.widgets.get("catalogo")

COM_DADO_EM_CLARO = ["lab_origem", "lab_log", "lab_bronze", "lab_controle", "lab_qualidade", "lab_leitores"]
PROTEGIDAS = ["lab_silver", "lab_gold"]
DONO = spark.sql("SELECT current_user() AS u").first().u


def rodar(cmd):
    try:
        spark.sql(cmd)
        return "ok"
    except Exception as e:
        msg = str(e)
        return "sem principal" if "PRINCIPAL_DOES_NOT_EXIST" in msg else "erro: " + msg.splitlines()[0][:150]


# Plano A: grupos da conta. Plano B: account users (analista) e o dono (pipeline).
if rodar(f"GRANT USE CATALOG ON CATALOG {CATALOGO} TO `lab_analistas`") == "ok":
    ANALISTA, PIPELINE, PLANO = "lab_analistas", "lab_pipeline", "A: grupos da conta"
else:
    ANALISTA, PIPELINE, PLANO = "account users", None, "B: account users como analista, dono como pipeline"
print(f"Plano {PLANO}")
print(f"  analista = `{ANALISTA}` · pipeline = {('`' + PIPELINE + '`') if PIPELINE else DONO + ' (dono dos schemas)'}")

# COMMAND ----------

COMANDOS = [f"GRANT USE CATALOG ON CATALOG {CATALOGO} TO `{ANALISTA}`"]
for s in PROTEGIDAS:
    COMANDOS.append(f"GRANT USE SCHEMA, SELECT ON SCHEMA {CATALOGO}.{s} TO `{ANALISTA}`")
for s in COM_DADO_EM_CLARO:
    COMANDOS.append(f"REVOKE ALL PRIVILEGES ON SCHEMA {CATALOGO}.{s} FROM `{ANALISTA}`")
if PIPELINE:
    COMANDOS.append(f"GRANT USE CATALOG ON CATALOG {CATALOGO} TO `{PIPELINE}`")
    for s in COM_DADO_EM_CLARO + PROTEGIDAS:
        COMANDOS.append(f"GRANT ALL PRIVILEGES ON SCHEMA {CATALOGO}.{s} TO `{PIPELINE}`")

falhas = 0
for cmd in COMANDOS:
    st = rodar(cmd)
    falhas += st != "ok"
    print(f"[{'ok' if st == 'ok' else 'ERRO'}] {cmd}" + ("" if st == "ok" else f"\n       {st}"))

# COMMAND ----------

def grants(objeto):
    try:
        return [(r.Principal, r.ActionType) for r in spark.sql(f"SHOW GRANTS ON {objeto}").collect()]
    except Exception as e:
        return [("(erro)", str(e).splitlines()[0][:120])]


def donos(schema):
    try:
        linhas = spark.sql(f"DESCRIBE SCHEMA EXTENDED {CATALOGO}.{schema}").collect()
        return next((r[1] for r in linhas if r[0].lower() == "owner"), "?")
    except Exception:
        return "?"


LEITURA = {"SELECT", "ALL PRIVILEGES"}
catalogo = grants(f"CATALOG {CATALOGO}")
print(f"CATALOG {CATALOGO}:", "; ".join(f"{p}: {a}" for p, a in catalogo) or "só o dono")
analista_le = []
for s in COM_DADO_EM_CLARO + PROTEGIDAS:
    g = grants(f"SCHEMA {CATALOGO}.{s}")
    print(f"SCHEMA {s:<14} dono: {donos(s):<30} " + ("; ".join(f"{p}: {a}" for p, a in g) or "nenhum GRANT"))
    if s in COM_DADO_EM_CLARO and any(p == ANALISTA and a in LEITURA for p, a in g):
        analista_le.append(s)
herdado = [p for p, a in catalogo if a in LEITURA]

print()
if herdado:
    print("⚠ Têm leitura no catálogo inteiro (e por herança leem a bronze):", ", ".join(herdado))
if analista_le or ANALISTA in herdado:
    raise Exception(f"`{ANALISTA}` ainda lê camada com dado pessoal: {', '.join(analista_le or ['catálogo'])}")
if falhas:
    raise Exception(f"{falhas} comando(s) de permissão falharam; vejam as linhas [ERRO] acima.")
print(f"Separação valendo: `{ANALISTA}` lê só {', '.join(PROTEGIDAS)}; "
      f"a bronze e as outras camadas com dado em claro ficam com {PIPELINE or 'o dono (' + DONO + ')'}.")
