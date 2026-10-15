# Databricks notebook source
# MAGIC %md
# MAGIC # Missão 4 · Verificação e evidência
# MAGIC
# MAGIC Confere as quatro verificações do laboratório e grava `evidencias/missao4.txt`. A evidência traz o
# MAGIC pedido e o motivo de cada linha em quarentena, mas não o conteúdo das linhas: a observação dos pedidos
# MAGIC pode ter dado pessoal.

# COMMAND ----------

dbutils.widgets.text("catalogo", "workspace", "Catálogo")
CATALOGO = dbutils.widgets.get("catalogo")
BRONZE, SILVER, QUALIDADE, CONTROLE = (f"{CATALOGO}.lab_bronze", f"{CATALOGO}.lab_silver",
                                       f"{CATALOGO}.lab_qualidade", f"{CATALOGO}.lab_controle")
ARQUIVO = "pedidos_dia3.csv"

import os
from datetime import datetime

def reais(c):
    return f"R$ {c / 100:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

lote = spark.sql(f"SELECT * FROM {BRONZE}.pedidos_lote WHERE _arquivo = '{ARQUIVO}' ORDER BY _linha").collect()
quarentena = spark.sql(f"""SELECT * FROM {QUALIDADE}.quarentena
                           WHERE conjunto = 'pedidos' AND origem = '{ARQUIVO}' ORDER BY CAST(linha AS INT)""").collect()
aceitos_do_lote = sorted(r.id for r in spark.sql(
    f"SELECT id FROM {SILVER}.pedidos WHERE _origem = '{ARQUIVO}'").collect())
silver = spark.sql(f"""SELECT COUNT(*) AS n, COALESCE(SUM(CASE WHEN status = 'pago' THEN valor_centavos END), 0) AS receita
                       FROM {SILVER}.pedidos""").first()
sem_validacao = spark.sql(f"""
  SELECT SUM(CASE WHEN status = 'pago' THEN CAST(valor_centavos AS BIGINT) END) AS receita FROM (
    SELECT CAST(valor_centavos AS STRING) AS valor_centavos, status FROM {BRONZE}.pedidos
    UNION ALL
    SELECT valor_centavos, status FROM {BRONZE}.pedidos_lote WHERE _arquivo = '{ARQUIVO}')""").first().receita
validacoes = spark.sql(f"SELECT * FROM {CONTROLE}.validacoes ORDER BY execucao, conjunto").collect()

CATEGORIAS = [("valor negativo", "valor_centavos abaixo do mínimo"), ("cliente ausente", "cliente_id obrigatório"),
              ("status fora do domínio", "status fora do domínio"), ("chave repetida", "id repetido"),
              ("data no futuro", "criado_em no futuro")]
def categoria(motivo):
    achadas = [nome for nome, trecho in CATEGORIAS if trecho in motivo]
    return achadas[0] if len(achadas) == 1 else ("várias: " + ", ".join(achadas) if achadas else "outro")

categorias = sorted(categoria(q.motivo) for q in quarentena)
contratos = {c: os.path.exists(os.path.abspath(os.path.join(os.getcwd(), "..", "contratos", f"{c}.yaml")))
             for c in ["pedidos", "clientes", "navegacao"]}

checks = [
    ("Do lote do dia 3, 2 linhas são aceitas (pedidos 110 e 115) e 5 vão para a quarentena",
     len(lote) == 7 and aceitos_do_lote == [110, 115] and len(quarentena) == 5,
     f"lote com {len(lote)} linhas · aceitos {aceitos_do_lote} · quarentena {len(quarentena)}"),
    ("Cada linha em quarentena traz um motivo diferente: valor negativo, cliente ausente, status fora do domínio, "
     "chave repetida e data no futuro",
     categorias == sorted(n for n, _ in CATEGORIAS),
     "; ".join(categorias)),
    ("Camada silver: 11 pedidos. Receita paga: R$ 844,90",
     (silver.n, silver.receita) == (11, 84490),
     f"{silver.n} pedidos · receita paga {reais(silver.receita)}"),
    ("Sem a validação, o mesmo cálculo daria R$ 932,90",
     sem_validacao == 93290, f"receita somando tudo o que chegou, sem contrato: {reais(sem_validacao)}"),
]

linhas = [f"Missão 4 · contrato, validação e quarentena · verificado em {datetime.now():%Y-%m-%d %H:%M}", ""]
for i, (desc, ok, det) in enumerate(checks, 1):
    linhas += [f"[{'OK' if ok else 'PENDENTE'}] {i}. {desc}", f"      {det}"]

linhas += ["", "Contratos lidos pelo validador: " +
           ", ".join(f"contratos/{c}.yaml" + ("" if ok else " (FALTANDO)") for c, ok in contratos.items()), "",
           f"Quarentena do lote {ARQUIVO} (lab_qualidade.quarentena):", "  linha  pedido  motivo"]
for q in quarentena:
    linhas.append(f"  {q.linha:<6} {str(q.chave or '(vazio)'):<7} {q.motivo} · execução {q.execucao}")

linhas += ["", "Execuções da validação (lab_controle.validacoes):"]
for v in validacoes:
    linhas.append(f"  execução {v.execucao} · {v.conjunto:<10} contrato v{v.versao_contrato}, {v.regras} regras · "
                  f"lidas {v.lidas}, aceitas {v.aceitas}, quarentena {v.quarentena}")
linhas += ["", "A diferença de R$ 88,00 entre os dois números é receita que não existe: valor negativo, pedido sem",
           "cliente, pedido de 2031 e o pedido 110 contado duas vezes. Sem o contrato, o pipeline terminaria sem erro."]

texto = "\n".join(linhas)
print(texto)
caminho = os.path.abspath(os.path.join(os.getcwd(), "..", "evidencias", "missao4.txt"))
with open(caminho, "w", encoding="utf-8") as f:
    f.write(texto + "\n")
print("\nEvidência gravada em", caminho)
