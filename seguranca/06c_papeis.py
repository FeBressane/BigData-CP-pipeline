# Databricks notebook source
# MAGIC %md
# MAGIC # Missão 6 · Papéis: o pipeline lê a bronze, o analista só a silver
# MAGIC
# MAGIC Os papéis são dois grupos do Unity Catalog, e as permissões ficam escritas aqui, como código:
# MAGIC
# MAGIC | Papel | Grupo | Pode ler | Não pode ler |
# MAGIC |---|---|---|---|
# MAGIC | Pipeline | `lab_pipeline` | origem, log, bronze, controle, qualidade, silver, gold | |
# MAGIC | Analista | `lab_analistas` | silver e gold | origem, log, bronze, controle, qualidade (quarentena e varredura) |
# MAGIC
# MAGIC O notebook cria os grupos (se a conta permitir), concede o que cada papel pode e **revoga** do analista tudo
# MAGIC nas camadas com dado pessoal em claro. Rodar de novo dá o mesmo resultado. No fim mostra as permissões que
# MAGIC ficaram valendo, inclusive as herdadas do catálogo, e avisa se alguém além do pipeline consegue ler a bronze.
# MAGIC
# MAGIC Na Free Edition há um usuário só (o dono do workspace, que é administrador e enxerga tudo). Por isso a separação
# MAGIC vale para quem entrar nos grupos: o próprio dono não serve de teste do papel de analista.

# COMMAND ----------

dbutils.widgets.text("catalogo", "workspace", "Catálogo")
CATALOGO = dbutils.widgets.get("catalogo")

PIPELINE, ANALISTAS = "lab_pipeline", "lab_analistas"
COM_DADO_EM_CLARO = ["lab_origem", "lab_log", "lab_bronze", "lab_controle", "lab_qualidade", "lab_leitores"]
PROTEGIDAS = ["lab_silver", "lab_gold"]

# 1. Grupos
grupos = {}
try:
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    for g in (PIPELINE, ANALISTAS):
        existentes = list(w.groups.list(filter=f'displayName eq "{g}"'))
        if existentes:
            grupos[g] = "já existia"
        else:
            w.groups.create(display_name=g)
            grupos[g] = "criado"
except Exception as e:
    for g in (PIPELINE, ANALISTAS):
        grupos.setdefault(g, f"não foi possível criar ({type(e).__name__}): criem em Settings > Identity and access")
for g, st in grupos.items():
    print(f"grupo {g}: {st}")

# COMMAND ----------

# 2. Permissões como código
COMANDOS = [f"GRANT USE CATALOG ON CATALOG {CATALOGO} TO `{PIPELINE}`",
            f"GRANT USE CATALOG ON CATALOG {CATALOGO} TO `{ANALISTAS}`"]
for s in COM_DADO_EM_CLARO + PROTEGIDAS:
    COMANDOS.append(f"GRANT ALL PRIVILEGES ON SCHEMA {CATALOGO}.{s} TO `{PIPELINE}`")
for s in PROTEGIDAS:
    COMANDOS.append(f"GRANT USE SCHEMA, SELECT ON SCHEMA {CATALOGO}.{s} TO `{ANALISTAS}`")
for s in COM_DADO_EM_CLARO:
    COMANDOS.append(f"REVOKE ALL PRIVILEGES ON SCHEMA {CATALOGO}.{s} FROM `{ANALISTAS}`")

resultado = []
for cmd in COMANDOS:
    try:
        spark.sql(cmd)
        resultado.append((cmd, "ok"))
    except Exception as e:
        resultado.append((cmd, "erro: " + str(e).splitlines()[0][:150]))
for cmd, st in resultado:
    print(f"[{st[:4]}] {cmd}" + ("" if st == "ok" else f"\n       {st}"))

# COMMAND ----------

# 3. O que ficou valendo
def grants(objeto):
    try:
        return [(r.Principal, r.ActionType) for r in spark.sql(f"SHOW GRANTS ON {objeto}").collect()]
    except Exception as e:
        return [("(erro)", str(e).splitlines()[0][:120])]

print(f"CATALOG {CATALOGO}:")
for p, a in grants(f"CATALOG {CATALOGO}"):
    print(f"   {p:<30} {a}")
leitores_bronze = set()
for s in COM_DADO_EM_CLARO + PROTEGIDAS:
    print(f"SCHEMA {s}:")
    for p, a in grants(f"SCHEMA {CATALOGO}.{s}"):
        print(f"   {p:<30} {a}")
        if s == "lab_bronze" and a in ("SELECT", "ALL PRIVILEGES"):
            leitores_bronze.add(p)
herdado = [p for p, a in grants(f"CATALOG {CATALOGO}") if a in ("SELECT", "ALL PRIVILEGES")]
print()
print("Leem a bronze por permissão direta:", ", ".join(sorted(leitores_bronze)) or "ninguém além do dono")
if herdado:
    print("⚠ Atenção: estes principais têm SELECT no catálogo inteiro e por isso leem a bronze por herança:",
          ", ".join(herdado))
if ANALISTAS in leitores_bronze:
    raise Exception(f"O grupo {ANALISTAS} ainda lê a bronze. A separação de papéis não está valendo.")
