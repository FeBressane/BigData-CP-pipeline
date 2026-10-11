# Databricks notebook source
# MAGIC %md
# MAGIC # Missão 2 · Verificação e evidência
# MAGIC
# MAGIC Confere as quatro verificações do laboratório e grava `evidencias/missao2.txt` com o
# MAGIC conteúdo do log. A coluna `dados` fica de fora da evidência: ela traz dado pessoal
# MAGIC (nome, e-mail, CPF), e a pasta de evidências não deve virar mais uma morada dele.

# COMMAND ----------

dbutils.widgets.text("catalogo", "workspace", "Catálogo")
CATALOGO = dbutils.widgets.get("catalogo")
BRONZE, CONTROLE, LOG = (f"{CATALOGO}.lab_bronze", f"{CATALOGO}.lab_controle", f"{CATALOGO}.lab_log.mudancas")

eventos = spark.sql(f"SELECT seq, tabela, operacao, chave, versao_origem, quando FROM {LOG} ORDER BY seq").collect()
sincs = spark.sql(f"SELECT * FROM {CONTROLE}.sincronizacoes ORDER BY execucao").collect()
display(spark.sql(f"SELECT * FROM {CONTROLE}.sincronizacoes ORDER BY execucao"))

# COMMAND ----------

import os
from datetime import datetime

def reais(c):
    return f"R$ {c / 100:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

ESPERADO_LOG = [("pedidos", "alteracao", 105), ("clientes", "alteracao", 3), ("clientes", "insercao", 7),
                ("pedidos", "insercao", 109), ("clientes", "exclusao", 5)]
obtido_log = [(e.tabela, e.operacao, e.chave) for e in eventos]

b = spark.sql(f"""
SELECT
  (SELECT COUNT(*) FROM {BRONZE}.clientes) AS clientes,
  (SELECT COUNT(*) FROM {BRONZE}.clientes WHERE id = 5) AS c5,
  (SELECT MAX(email) FROM {BRONZE}.clientes WHERE id = 3) AS email3,
  (SELECT COUNT(*) FROM {BRONZE}.pedidos) AS pedidos,
  (SELECT COUNT(*) FROM {BRONZE}.pedidos WHERE id = 106) AS p106,
  (SELECT COALESCE(SUM(valor_centavos), 0) FROM {BRONZE}.pedidos WHERE status = 'pago') AS receita
""").first()

normal = next((s for s in sincs if not s.reaplicou and s.aplicados > 0), None)
reapl = next((s for s in reversed(sincs) if s.reaplicou), None)
numeros = lambda s: (s.bronze_clientes, s.bronze_pedidos, s.receita_paga_centavos)

checks = [
    ("O log registra 5 eventos, na ordem em que foram aplicados",
     obtido_log == ESPERADO_LOG,
     f"{len(eventos)} evento(s): " + "; ".join(f"{t} {o} {k}" for t, o, k in obtido_log)),
    ("Bronze: 6 clientes (sem a cliente 5), 9 pedidos, e-mail novo no cliente 3",
     (b.clientes, b.c5, b.pedidos, b.email3) == (6, 0, 9, "carla.n@example.com"),
     f"{b.clientes} clientes, cliente 5 presente: {'sim' if b.c5 else 'não'}, {b.pedidos} pedidos, e-mail do 3: {b.email3}"),
    ("O pedido 106 continua lá. Receita paga: R$ 750,90",
     b.p106 == 1 and b.receita == 75090,
     f"pedido 106 presente: {'sim' if b.p106 else 'não'}, receita {reais(b.receita)}"),
    ("Reaplicar o log desde o primeiro evento: mesmos números",
     normal is not None and reapl is not None and numeros(reapl) == numeros(normal) == (6, 9, 75090),
     (f"sincronização {normal.execucao}: {numeros(normal)} · reaplicação {reapl.execucao}: {numeros(reapl)}"
      if normal and reapl else "rodem 02_sincronizar com 'Reaplicar desde o início' = sim")),
]

linhas = [f"Missão 2 · log de mudanças (Trilha B, Change Data Feed) · verificado em {datetime.now():%Y-%m-%d %H:%M}", ""]
for i, (desc, ok, detalhe) in enumerate(checks, 1):
    linhas += [f"[{'OK' if ok else 'PENDENTE'}] {i}. {desc}", f"      {detalhe}"]

linhas += ["", "Conteúdo do log (lab_log.mudancas). A coluna dados foi omitida: contém dado pessoal.",
           "  seq  tabela    operacao   chave  versão  quando"]
for e in eventos:
    linhas.append(f"  {e.seq:<4} {e.tabela:<9} {e.operacao:<10} {e.chave:<6} {e.versao_origem:<7} {e.quando:%Y-%m-%d %H:%M:%S}")

linhas += ["", "Sincronizações (lab_controle.sincronizacoes):"]
for s in sincs:
    linhas.append(f"  execução {s.execucao}{' (reaplicou desde o início)' if s.reaplicou else ''}: "
                  f"capturou {s.capturados}, aplicou {s.aplicados}, offset {s.offset_antes} -> {s.offset_depois}, "
                  f"bronze {s.bronze_clientes} / {s.bronze_pedidos}, receita {reais(s.receita_paga_centavos)}")

texto = "\n".join(linhas)
print(texto)
caminho = os.path.abspath(os.path.join(os.getcwd(), "..", "evidencias", "missao2.txt"))
with open(caminho, "w", encoding="utf-8") as f:
    f.write(texto + "\n")
print("\nEvidência gravada em", caminho)
