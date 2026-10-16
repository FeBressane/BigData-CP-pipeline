# Databricks notebook source
# MAGIC %md
# MAGIC # Missão 8 · Comando esquecer
# MAGIC
# MAGIC `esquecer(cliente_id)` trata **todas as moradas** do cliente (inventário em `privacidade/moradas.md`) e registra
# MAGIC uma linha na **lista de eliminação** (`lab_controle.eliminacoes`), sem nenhum dado pessoal: a chave da lista é o
# MAGIC pseudônimo `cliente_ps`, que sem a chave HMAC e sem a bronze não volta à pessoa. É essa chave que permite
# MAGIC reaplicar a lista depois de restaurar uma cópia antiga (Missão 9).
# MAGIC
# MAGIC Regras:
# MAGIC * **Cadastro, navegação e eventos** do cliente são apagados em todas as camadas.
# MAGIC * **Pedido** não é apagado: é registro de venda (retenção fiscal). Ele fica **sem pessoa**: `cliente_id` vazio,
# MAGIC   observação limpa, `cliente_ps` vazio na silver, e entra em `lab_controle.anonimizados` para o contrato aceitar.
# MAGIC   O valor continua na receita.
# MAGIC * **Idempotente:** rodar de novo encontra 0 linhas em cada morada e não muda nada.
# MAGIC
# MAGIC Quem chama precisa ter rodado antes `%run ../seguranca/06a_protecao` (pseudônimo).

# COMMAND ----------

import json
from datetime import datetime


def _dml(sql: str) -> int:
    linhas = spark.sql(sql).collect()
    if linhas and "num_affected_rows" in linhas[0].asDict():
        return int(linhas[0]["num_affected_rows"])
    return 0


def _existe(tabela: str) -> bool:
    try:
        return spark.catalog.tableExists(tabela)
    except Exception:
        return False


def _texto(v) -> str:
    return "'" + str(v).replace("\\", "\\\\").replace("'", "\\'") + "'"


def preparar_lista(catalogo: str):
    C = f"{catalogo}.lab_controle"
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {C}.eliminacoes (
      chave_ps STRING COMMENT 'Pseudônimo do cliente (HMAC); nunca o id, o nome ou o CPF',
      pedido_em TIMESTAMP, executado_em TIMESTAMP,
      moradas STRING COMMENT 'JSON: morada -> linhas tratadas na primeira execução'
    ) COMMENT 'Lista de eliminação (direito ao esquecimento). Reaplicada depois de toda restauração (Missão 9).'""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {C}.anonimizados (
      conjunto STRING, chave STRING COMMENT 'Chave da linha (ex.: id do pedido), nunca dado pessoal',
      motivo STRING, anonimizado_em TIMESTAMP
    ) COMMENT 'Linhas que ficaram sem pessoa por um pedido de eliminação (Missão 8)'""")


def tratar_moradas(catalogo: str, ps: str, cliente_id=None) -> dict:
    """Trata todas as moradas de um cliente. Com cliente_id, acha as linhas pelo id; sem ele (reaplicação da lista
    sobre uma cópia restaurada), acha pelo pseudônimo calculado sobre cada id. Devolve {morada: linhas tratadas}."""
    O, L, B, LE, Q, S, C = (f"{catalogo}.{s}" for s in ("lab_origem", "lab_log", "lab_bronze", "lab_leitores",
                                                         "lab_qualidade", "lab_silver", "lab_controle"))
    # condição "esta linha é do cliente": pelo id, ou pelo pseudônimo do id (pseudonimo() registrado pela 06a)
    def do_cliente(col, texto=False):
        if cliente_id is not None:
            return f"{col} = '{int(cliente_id)}'" if texto else f"{col} = {int(cliente_id)}"
        return f"pseudonimo(CAST(try_cast({col} AS INT) AS STRING)) = '{ps}'"

    feito = {}

    def passo(morada, tabela, sql):
        if _existe(tabela):
            feito[morada] = _dml(sql)

    # pedidos e eventos do cliente, antes de apagar (para as saídas dos leitores e para a lista de anonimizados)
    pedidos, eventos = set(), set()
    for t, col, chave in [(f"{O}.pedidos", "cliente_id", "id"), (f"{B}.pedidos", "cliente_id", "id"),
                          (f"{B}.pedidos_lote", "cliente_id", "id")]:
        if _existe(t):
            pedidos |= {str(r[0]) for r in spark.sql(
                f"SELECT CAST({chave} AS STRING) FROM {t} WHERE {do_cliente(col, t.endswith('_lote'))}").collect()}
    if _existe(f"{S}.pedidos"):
        pedidos |= {str(r[0]) for r in spark.sql(f"SELECT CAST(id AS STRING) FROM {S}.pedidos WHERE cliente_ps = '{ps}'").collect()}
    for t, col in [(f"{O}.navegacao", "cliente_id"), (f"{L}.navegacao", "cliente_id"), (f"{B}.navegacao", "cliente_id"),
                   (f"{LE}.por_cliente_eventos", "cliente_id")]:
        if _existe(t):
            eventos |= {int(r[0]) for r in spark.sql(f"SELECT evento_id FROM {t} WHERE {do_cliente(col)}").collect()}
    if _existe(f"{S}.navegacao"):
        eventos |= {int(r[0]) for r in spark.sql(f"SELECT evento_id FROM {S}.navegacao WHERE cliente_ps = '{ps}'").collect()}
    lista_eventos = ", ".join(str(e) for e in sorted(eventos)) or "NULL"

    # 1. origem (o banco da loja)
    passo("origem.clientes", f"{O}.clientes", f"DELETE FROM {O}.clientes WHERE {do_cliente('id')}")
    passo("origem.pedidos (sem pessoa)", f"{O}.pedidos",
          f"UPDATE {O}.pedidos SET cliente_id = NULL, observacao = NULL WHERE {do_cliente('cliente_id')}")
    passo("origem.navegacao", f"{O}.navegacao", f"DELETE FROM {O}.navegacao WHERE {do_cliente('cliente_id')}")

    # 2. log de mudanças: o evento continua (seq, operação e chave reaplicam a exclusão), sem os dados da pessoa
    if _existe(f"{L}.mudancas"):
        n = _dml(f"UPDATE {L}.mudancas SET dados = NULL WHERE tabela = 'clientes' AND dados IS NOT NULL "
                 f"AND {do_cliente('chave')}")
        cond = do_cliente("get_json_object(dados, '$.cliente_id')", True)
        for r in spark.sql(f"SELECT seq, dados FROM {L}.mudancas WHERE tabela = 'pedidos' AND dados IS NOT NULL "
                           f"AND {cond}").collect():
            d = json.loads(r.dados)
            d["cliente_id"], d["observacao"] = None, None
            n += _dml(f"UPDATE {L}.mudancas SET dados = {_texto(json.dumps(d, ensure_ascii=False))} WHERE seq = {r.seq}")
        feito["log.mudancas (dados apagados)"] = n
    # 3. tópico de navegação e estado dos leitores
    passo("log.navegacao (tópico)", f"{L}.navegacao", f"DELETE FROM {L}.navegacao WHERE {do_cliente('cliente_id')}")
    passo("leitores.visitas_eventos", f"{LE}.visitas_eventos",
          f"DELETE FROM {LE}.visitas_eventos WHERE evento_id IN ({lista_eventos})")
    passo("leitores.por_cliente_eventos", f"{LE}.por_cliente_eventos",
          f"DELETE FROM {LE}.por_cliente_eventos WHERE evento_id IN ({lista_eventos}) OR {do_cliente('cliente_id')}")

    # 4. bronze
    passo("bronze.clientes", f"{B}.clientes", f"DELETE FROM {B}.clientes WHERE {do_cliente('id')}")
    passo("bronze.pedidos (sem pessoa)", f"{B}.pedidos",
          f"UPDATE {B}.pedidos SET cliente_id = NULL, observacao = NULL WHERE {do_cliente('cliente_id')}")
    passo("bronze.pedidos_lote (sem pessoa)", f"{B}.pedidos_lote",
          f"UPDATE {B}.pedidos_lote SET cliente_id = NULL, observacao = NULL WHERE {do_cliente('cliente_id', True)}")
    passo("bronze.navegacao", f"{B}.navegacao", f"DELETE FROM {B}.navegacao WHERE {do_cliente('cliente_id')}")

    # 5. quarentena (o cliente aparece só como pseudônimo) e silver
    passo("qualidade.quarentena", f"{Q}.quarentena",
          f"UPDATE {Q}.quarentena SET chave = CASE WHEN chave = '{ps}' THEN NULL ELSE chave END, "
          f"dados = replace(dados, '\"{ps}\"', 'null') WHERE chave = '{ps}' OR contains(dados, '{ps}')")
    passo("silver.clientes", f"{S}.clientes", f"DELETE FROM {S}.clientes WHERE cliente_ps = '{ps}'")
    passo("silver.pedidos (sem pessoa)", f"{S}.pedidos", f"UPDATE {S}.pedidos SET cliente_ps = NULL WHERE cliente_ps = '{ps}'")
    passo("silver.navegacao", f"{S}.navegacao", f"DELETE FROM {S}.navegacao WHERE cliente_ps = '{ps}'")

    # 6. pedidos que ficaram sem pessoa: o contrato passa a aceitar o cliente vazio neles
    n = 0
    for p in sorted(pedidos):
        n += _dml(f"""MERGE INTO {C}.anonimizados a USING (SELECT 'pedidos' AS conjunto, '{p}' AS chave) n
                      ON a.conjunto = n.conjunto AND a.chave = n.chave
                      WHEN NOT MATCHED THEN INSERT (conjunto, chave, motivo, anonimizado_em)
                      VALUES (n.conjunto, n.chave, 'pedido de eliminação do cliente', current_timestamp())""")
    feito["controle.anonimizados (pedidos sem pessoa)"] = n
    return feito


def limpar_historico(catalogo: str) -> dict:
    """Tenta apagar já as versões antigas do Delta, que ainda guardam o dado. Sem permissão para RETAIN 0 HOURS
    (serverless), roda o VACUUM padrão: o histórico some quando passa o prazo de retenção do Delta (7 dias)."""
    tabelas = [f"{catalogo}.{t}" for t in (
        "lab_origem.clientes", "lab_origem.pedidos", "lab_origem.navegacao", "lab_log.mudancas", "lab_log.navegacao",
        "lab_leitores.visitas_eventos", "lab_leitores.por_cliente_eventos", "lab_bronze.clientes", "lab_bronze.pedidos",
        "lab_bronze.pedidos_lote", "lab_bronze.navegacao", "lab_qualidade.quarentena", "lab_silver.clientes",
        "lab_silver.pedidos", "lab_silver.navegacao")]
    imediato = True
    try:
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    except Exception:
        imediato = False
    resultado = {}
    for t in tabelas:
        if not _existe(t):
            continue
        try:
            if imediato:
                spark.sql(f"VACUUM {t} RETAIN 0 HOURS")
                resultado[t] = "histórico apagado agora"
                continue
        except Exception:
            imediato = False
        try:
            spark.sql(f"VACUUM {t}")
            resultado[t] = "VACUUM padrão: versões antigas somem em até 7 dias"
        except Exception as e:
            resultado[t] = "VACUUM não rodou: " + str(e).splitlines()[0][:80]
    return resultado


def esquecer(catalogo: str, cliente_id: int, limpar: bool = True) -> dict:
    preparar_lista(catalogo)
    ps = pseudonimo_de(str(int(cliente_id)), obter_chave())
    C = f"{catalogo}.lab_controle"
    ja = spark.sql(f"SELECT COUNT(*) AS n FROM {C}.eliminacoes WHERE chave_ps = '{ps}'").first().n > 0
    moradas = tratar_moradas(catalogo, ps, cliente_id)
    historico = limpar_historico(catalogo) if limpar and sum(moradas.values()) > 0 else {}
    if not ja:
        spark.sql(f"""INSERT INTO {C}.eliminacoes VALUES ('{ps}', current_timestamp(), current_timestamp(),
                      {_texto(json.dumps(moradas, ensure_ascii=False))})""")
    return {"chave_ps": ps, "ja_estava_na_lista": ja, "moradas": moradas, "historico": historico}


def reaplicar_lista(catalogo: str) -> dict:
    """Depois de restaurar uma cópia: trata de novo as moradas de cada pseudônimo da lista (Missão 9)."""
    registrar_pseudonimo()
    total = {}
    for r in spark.sql(f"SELECT chave_ps FROM {catalogo}.lab_controle.eliminacoes").collect():
        total[r.chave_ps[:12] + "…"] = tratar_moradas(catalogo, r.chave_ps)
    return total
