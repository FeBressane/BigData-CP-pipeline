# Databricks notebook source
# MAGIC %md
# MAGIC # Mudanças do dia 2 na origem
# MAGIC
# MAGIC Simula o aplicativo da loja alterando o banco. São 5 mudanças, nesta ordem
# MAGIC (`dados/dia2/mudancas_dia2.txt`). Cada alteração ou inserção grava em `atualizado_em`
# MAGIC o horário indicado no laboratório.
# MAGIC
# MAGIC | # | Operação | Tabela | Chave | O quê | atualizado_em |
# MAGIC |---|---|---|---|---|---|
# MAGIC | 1 | alteração | pedidos | 105 | status pendente → pago | 2026-10-02 08:10 |
# MAGIC | 2 | alteração | clientes | 3 | e-mail carla.n@example.com | 2026-10-02 08:20 |
# MAGIC | 3 | inserção | clientes | 7 | Gabi Rocha | 2026-10-02 08:30 |
# MAGIC | 4 | inserção | pedidos | 109 | cliente 7, R$ 56,00, pago | 2026-10-02 08:40 |
# MAGIC | 5 | exclusão | clientes | 5 | a cliente pediu a eliminação; o pedido 106 permanece | — |
# MAGIC
# MAGIC Rodar duas vezes não duplica nada: cada passo confere o estado antes de mudar.

# COMMAND ----------

dbutils.widgets.text("catalogo", "workspace", "Catálogo")
ORIGEM = f"{dbutils.widgets.get('catalogo')}.lab_origem"

MUDANCAS = [
    ("1 · pedidos 105 pago",
     f"""UPDATE {ORIGEM}.pedidos
         SET status = 'pago', atualizado_em = TIMESTAMP '2026-10-02 08:10:00'
         WHERE id = 105 AND status <> 'pago'"""),
    ("2 · e-mail do cliente 3",
     f"""UPDATE {ORIGEM}.clientes
         SET email = 'carla.n@example.com', atualizado_em = TIMESTAMP '2026-10-02 08:20:00'
         WHERE id = 3 AND email <> 'carla.n@example.com'"""),
    ("3 · insere cliente 7",
     f"""INSERT INTO {ORIGEM}.clientes
         SELECT 7, 'Gabi Rocha', 'gabi.rocha@example.com', '777.777.777-77',
                DATE '1996-02-25', 'Sorocaba', TIMESTAMP '2026-10-02 08:30:00'
         WHERE NOT EXISTS (SELECT 1 FROM {ORIGEM}.clientes WHERE id = 7)"""),
    ("4 · insere pedido 109",
     f"""INSERT INTO {ORIGEM}.pedidos
         SELECT 109, 7, 5600, 'pago', TIMESTAMP '2026-10-02 08:40:00',
                TIMESTAMP '2026-10-02 08:40:00', NULL
         WHERE NOT EXISTS (SELECT 1 FROM {ORIGEM}.pedidos WHERE id = 109)"""),
    ("5 · exclui cliente 5",
     f"DELETE FROM {ORIGEM}.clientes WHERE id = 5"),
]

for nome, sql in MUDANCAS:
    afetadas = spark.sql(sql).first()[0]
    print(f"{nome}: {afetadas} linha(s)")

# COMMAND ----------

r = spark.sql(f"""
SELECT
  (SELECT COUNT(*) FROM {ORIGEM}.clientes) AS clientes,
  (SELECT COUNT(*) FROM {ORIGEM}.pedidos)  AS pedidos,
  (SELECT COUNT(*) FROM {ORIGEM}.pedidos WHERE id = 106) AS pedido_106
""").first()
print(f"Origem depois do dia 2: {r.clientes} clientes (esperado 6), {r.pedidos} pedidos (esperado 9), "
      f"pedido 106 presente: {'sim' if r.pedido_106 else 'NÃO'}")
assert (r.clientes, r.pedidos, r.pedido_106) == (6, 9, 1), "A origem não ficou no estado esperado do dia 2"
