# Databricks notebook source
# MAGIC %md
# MAGIC # Missão 7 · Verificação e evidência
# MAGIC
# MAGIC Confere as quatro verificações do laboratório e grava `evidencias/missao7.txt`:
# MAGIC 1. 4 combinações cidade + faixa etária com uma pessoa só.
# MAGIC 2. K = 2: Santos R$ 545,00 (3 clientes), Sorocaba R$ 95,90 (2), Campinas suprimida (1).
# MAGIC 3. O total geral continua R$ 844,90, incluindo a suprimida e os R$ 159,00 do pedido 106.
# MAGIC 4. Com K = 3, Sorocaba também é suprimida.
# MAGIC
# MAGIC Também confere que todo campo pessoal que ficou na silver tem finalidade registrada. Rodem antes
# MAGIC `qualidade/04b_validar` e `privacidade/07a_visao_consumo`.

# COMMAND ----------

# MAGIC %run ../governanca/05a_governanca

# COMMAND ----------

# MAGIC %run ./07_consumo_comum

# COMMAND ----------

from datetime import datetime
from decimal import Decimal

dbutils.widgets.text("catalogo", "workspace", "Catálogo")
CATALOGO = dbutils.widgets.get("catalogo")
SILVER, GOLD = f"{CATALOGO}.lab_silver", f"{CATALOGO}.lab_gold"


def reais(v):
    return f"R$ {Decimal(v or 0):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


k_medido, unicas, grupos = medir_k(SILVER)
visao = spark.table(f"{GOLD}.receita_por_cidade").collect()          # a visão publicada (K = 2)
k3 = spark.sql(sql_receita_por_cidade(SILVER, 3)).collect()           # mesma consulta com K = 3
por_nome = lambda linhas: {r.cidade: r for r in linhas}
v2, v3 = por_nome(visao), por_nome(k3)
total = sum(Decimal(r.receita_paga) for r in visao)
total_k3 = sum(Decimal(r.receita_paga) for r in k3)
pedido_106 = spark.sql(f"""SELECT CAST(SUM(p.valor_centavos) / 100.0 AS DECIMAL(12,2)) AS v
                           FROM {SILVER}.pedidos p LEFT ANTI JOIN {SILVER}.clientes c ON p.cliente_ps = c.cliente_ps
                           WHERE p.status = 'pago'""").first().v
campinas = spark.sql(f"SELECT COUNT(*) AS n FROM {SILVER}.clientes WHERE cidade = 'Campinas'").first().n

esperado_k2 = {"Santos": (3, Decimal("545.00")), "Sorocaba": (2, Decimal("95.90"))}
ok_k2 = (all(c in v2 and (v2[c].clientes, Decimal(v2[c].receita_paga)) == e for c, e in esperado_k2.items())
         and "Campinas" not in v2 and SUPRIMIDAS in v2 and campinas == 1)

# finalidade de cada campo pessoal que ficou na silver
contratos = ler_contratos()
na_silver = [(n, f["nome"], f.get("protecao"), f.get("finalidade")) for n, c in contratos.items()
             for f in c["campos"] if f.get("classe") in CLASSES_COM_RETENCAO and f.get("protecao") != "remover"]
sem_finalidade = [f"{n}.{c}" for n, c, _, fin in na_silver if not fin]
colunas_clientes = spark.table(f"{SILVER}.clientes").columns

checks = [
    ("4 combinações cidade + faixa etária com uma pessoa só",
     unicas == 4,
     f"{len(grupos)} combinações · K = {k_medido} · {unicas} com uma pessoa só"),
    ("K = 2: Santos R$ 545,00 (3), Sorocaba R$ 95,90 (2), Campinas suprimida (1)",
     ok_k2,
     " · ".join(f"{r.cidade if r.cidade != SUPRIMIDAS else 'outras (suprimidas + sem cliente)'}: "
                f"{reais(r.receita_paga)}" + (f" ({r.clientes})" if r.clientes is not None else "") for r in visao)
     + f" · Campinas tem {campinas} cliente e não aparece pelo nome"),
    ("Total geral continua R$ 844,90 (inclui a suprimida e os R$ 159,00 do pedido 106)",
     total == Decimal("844.90") and Decimal(pedido_106 or 0) == Decimal("159.00"),
     f"soma da visão: {reais(total)} · pedidos pagos sem cliente (pedido 106): {reais(pedido_106)}"),
    ("Com K = 3, Sorocaba também é suprimida",
     "Sorocaba" not in v3 and "Santos" in v3 and total_k3 == total,
     " · ".join(f"{r.cidade if r.cidade != SUPRIMIDAS else 'outras'}: {reais(r.receita_paga)}" for r in k3)
     + f" · total {reais(total_k3)}"),
]

linhas = [f"Missão 7 · pseudônimo não é anônimo, visão de consumo · verificado em {datetime.now():%Y-%m-%d %H:%M}", ""]
for i, (desc, ok, det) in enumerate(checks, 1):
    linhas += [f"[{'OK' if ok else 'PENDENTE'}] {i}. {desc}", f"      {det}"]

linhas += ["", "Medição de K (lab_silver.clientes, quase-identificadores cidade + faixa_etaria):",
           "  cidade      faixa     pessoas"]
for g in grupos:
    linhas.append(f"  {g.cidade:<11} {g.faixa_etaria:<9} {g.pessoas}")
linhas += [f"  K = {k_medido}: há pessoas sozinhas na sua combinação. Quem souber a cidade e a idade de alguém",
           "  acha a linha dela, mesmo sem nome e com pseudônimo. Por isso a visão de consumo agrega e suprime."]

linhas += ["", "Visão gold lab_gold.receita_por_cidade (K = 2):", "  cidade                                        clientes  receita paga"]
for r in visao:
    linhas.append(f"  {r.cidade:<45} {str(r.clientes if r.clientes is not None else '—'):>8}  {reais(r.receita_paga)}")

linhas += ["", f"Campos pessoais na silver e finalidade (colunas atuais de clientes: "
           f"{', '.join(c for c in colunas_clientes if not c.startswith('_'))}):"]
for n, c, prot, fin in na_silver:
    linhas.append(f"  {n}.{c:<11} {prot:<12} {fin or 'SEM FINALIDADE'}")
linhas.append(f"  campos pessoais na silver sem finalidade: {', '.join(sem_finalidade) or 'nenhum'}")

texto = "\n".join(linhas)
print(texto)
caminho = os.path.join(RAIZ_REPO, "evidencias", "missao7.txt")
with open(caminho, "w", encoding="utf-8") as f:
    f.write(texto + "\n")
print("\nEvidência gravada em", caminho)
