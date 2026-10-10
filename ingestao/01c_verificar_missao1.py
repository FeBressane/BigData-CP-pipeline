# Databricks notebook source
# MAGIC %md
# MAGIC # Missão 1 · Verificação e evidência
# MAGIC
# MAGIC Confere as cinco verificações do laboratório a partir do histórico em
# MAGIC `lab_controle.execucoes` e do estado atual da bronze, e grava `evidencias/missao1.txt`.
# MAGIC
# MAGIC Rodem depois da sequência: carga → carga de novo → `01b_aplicar_dia2` → carga.

# COMMAND ----------

dbutils.widgets.text("catalogo", "workspace", "Catálogo")
CATALOGO = dbutils.widgets.get("catalogo")
ORIGEM, BRONZE, CONTROLE = (f"{CATALOGO}.lab_origem", f"{CATALOGO}.lab_bronze", f"{CATALOGO}.lab_controle")

execs = spark.sql(f"""
  SELECT *, lidas_clientes + lidas_pedidos + lidas_navegacao AS lidas
  FROM {CONTROLE}.execucoes WHERE missao = 'missao1' ORDER BY execucao
""").collect()
display(spark.sql(f"SELECT * FROM {CONTROLE}.execucoes WHERE missao = 'missao1' ORDER BY execucao"))

# COMMAND ----------

import os
from datetime import datetime

def reais(c):
    return f"R$ {c / 100:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

e1 = execs[0] if execs else None
e2 = next((e for e in execs[1:] if e.lidas == 0), None)
e3 = next((e for e in execs if e.lidas_clientes == 2 and e.lidas_pedidos == 2), None)

atual = spark.sql(f"""
SELECT
  (SELECT COUNT(*) FROM {BRONZE}.clientes) AS clientes,
  (SELECT COUNT(*) FROM {BRONZE}.pedidos)  AS pedidos,
  (SELECT COALESCE(SUM(valor_centavos), 0) FROM {BRONZE}.pedidos WHERE status = 'pago') AS receita,
  (SELECT COUNT(*) FROM {BRONZE}.clientes WHERE id = 5) AS c5_bronze,
  (SELECT COUNT(*) FROM {ORIGEM}.clientes  WHERE id = 5) AS c5_origem
""").first()

checks = [
    ("1ª execução: 6 clientes, 8 pedidos e 8 eventos na bronze",
     e1 is not None and (e1.bronze_clientes, e1.bronze_pedidos, e1.bronze_eventos) == (6, 8, 8),
     f"execução {e1.execucao}: {e1.bronze_clientes} / {e1.bronze_pedidos} / {e1.bronze_eventos}" if e1 else "sem execução"),
    ("2ª execução sem mudança: lê 0 linhas e os totais continuam 6, 8 e 8",
     e2 is not None and (e2.bronze_clientes, e2.bronze_pedidos, e2.bronze_eventos) == (6, 8, 8),
     f"execução {e2.execucao}: leu {e2.lidas}, totais {e2.bronze_clientes} / {e2.bronze_pedidos} / {e2.bronze_eventos}" if e2 else "nenhuma execução leu 0 linhas"),
    ("Depois do dia 2: lê 4 linhas (pedidos 105 e 109, clientes 3 e 7)",
     e3 is not None and e3.lidas == 4,
     f"execução {e3.execucao}: clientes {e3.lidas_clientes}, pedidos {e3.lidas_pedidos}, navegação {e3.lidas_navegacao}" if e3 else "dia 2 ainda não carregado"),
    ("Bronze: 7 clientes, 9 pedidos, receita paga de R$ 750,90",
     (atual.clientes, atual.pedidos, atual.receita) == (7, 9, 75090),
     f"agora: {atual.clientes} clientes, {atual.pedidos} pedidos, {reais(atual.receita)}"),
    ("Cliente 5 excluída na origem e ainda presente na bronze",
     atual.c5_origem == 0 and atual.c5_bronze == 1,
     f"origem: {atual.c5_origem} linha, bronze: {atual.c5_bronze} linha"),
]

linhas = [f"Missão 1 · carga incremental · verificado em {datetime.now():%Y-%m-%d %H:%M}", ""]
for i, (desc, ok, detalhe) in enumerate(checks, 1):
    linhas.append(f"[{'OK' if ok else 'PENDENTE'}] {i}. {desc}")
    linhas.append(f"      {detalhe}")
linhas += ["", "Execuções registradas em lab_controle.execucoes:"]
for e in execs:
    linhas.append(f"  execução {e.execucao}: leu {e.lidas} "
                  f"(clientes {e.lidas_clientes}, pedidos {e.lidas_pedidos}, navegação {e.lidas_navegacao}) "
                  f"-> bronze {e.bronze_clientes} / {e.bronze_pedidos} / {e.bronze_eventos}, receita {reais(e.receita_paga_centavos)}")
linhas += ["",
           "Observação: a cliente 5 continua na bronze porque a marca-d'água só enxerga linhas que existem.",
           "Um DELETE não deixa linha para ler. É o ponto de partida da Missão 2 (log de mudanças)."]

texto = "\n".join(linhas)
print(texto)

caminho = os.path.abspath(os.path.join(os.getcwd(), "..", "evidencias", "missao1.txt"))
with open(caminho, "w", encoding="utf-8") as f:
    f.write(texto + "\n")
print("\nEvidência gravada em", caminho)
