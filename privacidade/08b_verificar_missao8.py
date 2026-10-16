# Databricks notebook source
# MAGIC %md
# MAGIC # Missão 8 · Verificação e evidência
# MAGIC
# MAGIC Confere as quatro verificações do laboratório e grava `evidencias/missao8.txt`:
# MAGIC 1. Buscas por "Elisa", 555.555.555-55, (13) 90000-0005 e 203.0.113.55 na origem, log, bronze e silver (e nos
# MAGIC    outros schemas do laboratório): nada.
# MAGIC 2. Depois: 6 eventos, 11 pedidos, receita R$ 844,90; visão de consumo igual à da Missão 7.
# MAGIC 3. Lista de eliminação com 1 registro e nenhum dado pessoal.
# MAGIC 4. Rodar `esquecer 5` de novo não falha nem muda nada (o teste roda o comando de novo aqui).
# MAGIC
# MAGIC Rodem antes `08a_esquecer` e `qualidade/04b_validar`. A evidência não traz nenhum dado pessoal: os termos
# MAGIC buscados aparecem só como "termo 1..4".

# COMMAND ----------

# MAGIC %run ../governanca/05a_governanca

# COMMAND ----------

# MAGIC %run ../seguranca/06a_protecao

# COMMAND ----------

# MAGIC %run ./07_consumo_comum

# COMMAND ----------

# MAGIC %run ./08_esquecer_comum

# COMMAND ----------

import re
from datetime import datetime
from decimal import Decimal
from pyspark.sql import functions as F

dbutils.widgets.text("catalogo", "workspace", "Catálogo")
CATALOGO = dbutils.widgets.get("catalogo")
SILVER, GOLD, CONTROLE = f"{CATALOGO}.lab_silver", f"{CATALOGO}.lab_gold", f"{CATALOGO}.lab_controle"
SCHEMAS = ["lab_origem", "lab_log", "lab_bronze", "lab_silver", "lab_leitores", "lab_qualidade", "lab_controle", "lab_gold"]
TERMOS = ["Elisa", "555.555.555-55", "(13) 90000-0005", "203.0.113.55"]
PS5 = pseudonimo_de("5", obter_chave())


def reais(v):
    return f"R$ {Decimal(v or 0):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


# 1. Buscas, tabela por tabela, coluna de texto por coluna de texto (também o pseudônimo da cliente 5)
achados, tabelas_lidas = [], 0
for schema in SCHEMAS:
    try:
        tabelas = [r.tableName for r in spark.sql(f"SHOW TABLES IN {CATALOGO}.{schema}").collect() if not r.isTemporary]
    except Exception:
        continue
    for t in tabelas:
        df = spark.table(f"{CATALOGO}.{schema}.{t}")
        tabelas_lidas += 1
        textos = [f.name for f in df.schema.fields if f.dataType.simpleString() == "string"]
        # a lista de eliminação guarda o pseudônimo de propósito (é a chave dela): lá só se buscam os 4 termos
        termos = TERMOS if f"{schema}.{t}" == "lab_controle.eliminacoes" else TERMOS + [PS5]
        for i, termo in enumerate(termos, 1):
            for col in textos:
                n = df.where(F.col(col).contains(termo)).count()
                if n:
                    achados.append((f"{schema}.{t}.{col}", f"termo {i}" if i <= 4 else "pseudônimo da cliente 5", n))
busca_ok = not achados and tabelas_lidas > 0

# arquivos do repositório (fora dados/, que é a entrada fictícia do laboratório)
arquivos_com_termo = []
for raiz, pastas, arquivos in os.walk(RAIZ_REPO):
    pastas[:] = [p for p in pastas if p not in (".git", "__pycache__", "dados")]
    for a in arquivos:
        if a.endswith((".txt", ".md", ".yaml")):
            texto = open(os.path.join(raiz, a), encoding="utf-8", errors="ignore").read()
            if any(t in texto for t in TERMOS[1:]):
                arquivos_com_termo.append(os.path.relpath(os.path.join(raiz, a), RAIZ_REPO))

# 2. Números depois do esquecimento
eventos = spark.table(f"{SILVER}.navegacao").count()
ped = spark.sql(f"""SELECT COUNT(*) AS n, CAST(SUM(CASE WHEN status = 'pago' THEN valor_centavos END) / 100.0 AS DECIMAL(12,2)) AS r
                    FROM {SILVER}.pedidos""").first()
p106 = spark.sql(f"SELECT cliente_ps, valor_centavos, status FROM {SILVER}.pedidos WHERE id = 106").first()
visao = {r.cidade: (r.clientes, Decimal(r.receita_paga)) for r in spark.table(f"{GOLD}.receita_por_cidade").collect()}
visao_esperada = {"Santos": (3, Decimal("545.00")), "Sorocaba": (2, Decimal("95.90")), SUPRIMIDAS: (None, Decimal("204.00"))}
numeros_ok = (eventos == 6 and ped.n == 11 and Decimal(ped.r) == Decimal("844.90") and visao == visao_esperada
              and p106 is not None and p106.cliente_ps is None and p106.valor_centavos == 15900)

# 3. Lista de eliminação
lista = spark.table(f"{CONTROLE}.eliminacoes").collect()
texto_lista = " ".join(str(v) for r in lista for v in r.asDict().values())
pessoal_na_lista = [n for n, rx in PADROES_PESSOAIS.items() if re.search(rx, texto_lista)] + \
                   [f"termo {i}" for i, t in enumerate(TERMOS, 1) if t in texto_lista]
lista_ok = len(lista) == 1 and not pessoal_na_lista
anonimizados = sorted(r.chave for r in spark.sql(f"SELECT chave FROM {CONTROLE}.anonimizados WHERE conjunto = 'pedidos'").collect())

# 4. Rodar de novo: nada muda
antes = (eventos, ped.n, str(ped.r), len(lista))
segunda = esquecer(CATALOGO, 5, limpar=False)
lista2 = spark.table(f"{CONTROLE}.eliminacoes").count()
depois = (spark.table(f"{SILVER}.navegacao").count(), spark.table(f"{SILVER}.pedidos").count(), str(ped.r), lista2)
idempotente_ok = segunda["ja_estava_na_lista"] and sum(segunda["moradas"].values()) == 0 and lista2 == 1 \
                 and antes[:2] == depois[:2]

# histórico do Delta: a versão mais antiga ainda legível da origem guarda a cliente 5?
try:
    versoes = sorted(r.version for r in spark.sql(f"DESCRIBE HISTORY {CATALOGO}.lab_origem.clientes").collect())
    antiga = spark.sql(f"SELECT COUNT(*) AS n FROM {CATALOGO}.lab_origem.clientes VERSION AS OF {versoes[0]} "
                       f"WHERE nome LIKE '{TERMOS[0]}%'").first().n
    historico = (f"versão mais antiga legível de lab_origem.clientes: v{versoes[0]} · "
                 + ("ainda guarda a cliente: some no VACUUM, em até 7 dias" if antiga else "sem a cliente"))
except Exception as e:
    historico = "versões antigas já não podem ser lidas (VACUUM): " + str(e).splitlines()[0][:80]

checks = [
    ("Buscas pelos 4 termos da cliente 5 na origem, log, bronze e silver: nada",
     busca_ok,
     f"{tabelas_lidas} tabelas e visões em {len(SCHEMAS)} schemas · ocorrências: "
     + ("; ".join(f"{a} ({d}, {n})" for a, d, n in achados)
        or "nenhuma; o pseudônimo dela só existe como chave da lista de eliminação")),
    ("Depois: 6 eventos, 11 pedidos, receita R$ 844,90; visão de consumo igual",
     numeros_ok,
     f"{eventos} eventos · {ped.n} pedidos · receita paga {reais(ped.r)} · pedido 106: cliente_ps "
     f"{'vazio' if p106 and p106.cliente_ps is None else 'PREENCHIDO'}, {reais((p106.valor_centavos if p106 else 0) / 100)} · "
     f"visão: {'igual à da Missão 7' if visao == visao_esperada else 'DIFERENTE'}"),
    ("Lista de eliminação com 1 registro e nenhum dado pessoal",
     lista_ok,
     f"{len(lista)} registro(s) · dado pessoal na lista: {', '.join(pessoal_na_lista) or 'nenhum'}"),
    ("Rodar esquecer 5 de novo não falha nem muda nada",
     idempotente_ok,
     f"segunda execução: {sum(segunda['moradas'].values())} linhas tratadas · já estava na lista: "
     f"{'sim' if segunda['ja_estava_na_lista'] else 'não'} · lista com {lista2} registro(s)"),
]

linhas = [f"Missão 8 · direito ao esquecimento · verificado em {datetime.now():%Y-%m-%d %H:%M}", ""]
for i, (desc, ok, det) in enumerate(checks, 1):
    linhas += [f"[{'OK' if ok else 'PENDENTE'}] {i}. {desc}", f"      {det}"]

linhas += ["", "Lista de eliminação (lab_controle.eliminacoes):"]
for r in lista:
    linhas.append(f"  chave {r.chave_ps[:12]}… (pseudônimo HMAC) · pedido em {r.pedido_em:%Y-%m-%d %H:%M} · "
                  f"executado em {r.executado_em:%Y-%m-%d %H:%M}")
    for morada, n in json.loads(r.moradas).items():
        linhas.append(f"    {morada:<45} {n} linha(s)")
linhas += ["", f"Pedidos que ficaram sem pessoa (lab_controle.anonimizados): {', '.join(anonimizados) or 'nenhum'}",
           f"Histórico do Delta: {historico}",
           f"Arquivos do repositório com os termos (fora de dados/): {', '.join(arquivos_com_termo) or 'nenhum'}",
           "Exceção registrada: dados/dia1 é a entrada fictícia do laboratório e continua com a cliente; a lista de",
           "eliminação é reaplicada sempre que essa entrada ou uma cópia antiga for carregada de novo (Missão 9)."]

texto = "\n".join(linhas)
print(texto)
caminho = os.path.join(RAIZ_REPO, "evidencias", "missao8.txt")
with open(caminho, "w", encoding="utf-8") as f:
    f.write(texto + "\n")
print("\nEvidência gravada em", caminho)
