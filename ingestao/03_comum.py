# Databricks notebook source
# MAGIC %md
# MAGIC # Missão 3 · Peças comuns do tópico e dos leitores
# MAGIC
# MAGIC Este notebook não é rodado sozinho. Os outros notebooks da Missão 3 o carregam com `%run ./03_comum`.
# MAGIC
# MAGIC * **Tópico:** `lab_log.navegacao`, uma tabela em que só se anexa, com `seq` por evento (Trilha A, sem broker).
# MAGIC * **Offsets:** cada leitor guarda o seu em `lab_controle.offsets` (consumidor = nome do leitor, fonte = navegacao).
# MAGIC * **Histórico:** cada rodada de cada leitor vira uma linha em `lab_controle.leituras`.
# MAGIC
# MAGIC **Decisão sobre o offset:** o leitor salva o offset **depois** de processar. Se cair entre processar e
# MAGIC salvar, ao voltar ele lê de novo o mesmo trecho: entrega **pelo menos uma vez**. A contagem não fica dupla
# MAGIC porque o processamento é idempotente: cada leitor registra o `evento_id` que já contou e ignora repetidos.

# COMMAND ----------

dbutils.widgets.text("catalogo", "workspace", "Catálogo")
CATALOGO = dbutils.widgets.get("catalogo")
ORIGEM, BRONZE, CONTROLE, LOGS, LEITORES = (f"{CATALOGO}.lab_origem", f"{CATALOGO}.lab_bronze",
                                            f"{CATALOGO}.lab_controle", f"{CATALOGO}.lab_log",
                                            f"{CATALOGO}.lab_leitores")
TOPICO = f"{LOGS}.navegacao"

import time
from datetime import datetime


def ler_offset(consumidor: str, fonte: str = "navegacao"):
    r = spark.sql(f"SELECT posicao FROM {CONTROLE}.offsets "
                  f"WHERE consumidor = '{consumidor}' AND fonte = '{fonte}'").first()
    return r.posicao if r else None


def gravar_offset(consumidor: str, posicao: int, fonte: str = "navegacao") -> None:
    spark.sql(f"""
      MERGE INTO {CONTROLE}.offsets o
      USING (SELECT '{consumidor}' AS consumidor, '{fonte}' AS fonte, {posicao} AS posicao) n
      ON o.consumidor = n.consumidor AND o.fonte = n.fonte
      WHEN MATCHED THEN UPDATE SET o.posicao = n.posicao, o.atualizado_em = current_timestamp()
      WHEN NOT MATCHED THEN INSERT (consumidor, fonte, posicao, atualizado_em)
        VALUES (n.consumidor, n.fonte, n.posicao, current_timestamp())
    """)


def ultimo_seq() -> int:
    return spark.sql(f"SELECT COALESCE(MAX(seq), 0) AS m FROM {TOPICO}").first().m


def agora() -> str:
    return datetime.now().strftime("%H:%M:%S")


def rodar_leitor(nome: str, processar, intervalo: float, ocioso_max: float = 20,
                 espera_inicial: float = 180, cair_no_seq: int = 0) -> None:
    """Laço de um leitor: acorda a cada `intervalo` segundos, lê o que chegou depois do seu offset,
    processa e só então salva o offset. Para sozinho depois de `ocioso_max` segundos sem eventos novos.
    `cair_no_seq` > 0 simula uma queda: processa até esse seq e cai ANTES de salvar o offset."""
    rodada = spark.sql(f"SELECT COALESCE(MAX(rodada), 0) + 1 AS n FROM {CONTROLE}.leituras "
                       f"WHERE leitor = '{nome}'").first().n
    inicio = datetime.now()
    offset_inicial = ler_offset(nome) or 0
    offset = offset_inicial
    lidos = repetidos = atraso_max = 0
    caiu, viu_evento = False, False
    t0 = ultimo_novo = time.time()
    print(f"[{agora()}] {nome} · rodada {rodada} · começa no offset {offset}")
    try:
        while True:
            topo = ultimo_seq()
            atraso_max = max(atraso_max, topo - offset)  # quantos eventos encontrou pendentes ao acordar
            if topo > offset:
                vai_cair = cair_no_seq and offset < cair_no_seq <= topo
                ate = cair_no_seq if vai_cair else topo
                trecho = spark.table(TOPICO).where(f"seq > {offset} AND seq <= {ate}")
                n, rep = processar(trecho, rodada)
                lidos += n
                repetidos += rep
                extra = f" · {rep} repetido(s) ignorado(s)" if rep else ""
                if vai_cair:
                    caiu = True
                    print(f"[{agora()}] {nome} processou seq {offset + 1}..{ate}{extra} e CAIU antes de salvar o offset "
                          f"(o offset salvo continua {offset})")
                    raise RuntimeError(f"Queda simulada do leitor {nome} no seq {ate}. "
                                       f"Mude 'Cair no evento' para 0 e rode de novo para religar.")
                offset = ate
                gravar_offset(nome, offset)
                print(f"[{agora()}] {nome} leu seq {offset - n + 1}..{offset} ({n} evento(s)){extra} "
                      f"· offset salvo {offset} · encontrou {topo - (offset - n)} pendente(s) ao acordar")
                viu_evento, ultimo_novo = True, time.time()
            if viu_evento and time.time() - ultimo_novo > ocioso_max:
                print(f"[{agora()}] {nome} · {ocioso_max:.0f} s sem eventos novos, encerrando")
                break
            if not viu_evento and time.time() - t0 > espera_inicial:
                print(f"[{agora()}] {nome} · nenhum evento em {espera_inicial:.0f} s, encerrando")
                break
            time.sleep(intervalo)
    finally:
        spark.sql(f"""
          INSERT INTO {CONTROLE}.leituras VALUES (
            '{nome}', {rodada}, TIMESTAMP '{inicio:%Y-%m-%d %H:%M:%S}', current_timestamp(),
            {offset_inicial}, {offset}, {lidos}, {repetidos}, {atraso_max}, {str(caiu).lower()})
        """)
