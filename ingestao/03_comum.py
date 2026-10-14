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


def agora() -> str:
    return datetime.now().strftime("%H:%M:%S")


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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preparar o tópico e publicar

# COMMAND ----------

def preparar_topico() -> None:
    """Tópico vazio, offsets e histórico dos leitores apagados, saídas vazias, bronze.navegacao vazia."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {LEITORES} COMMENT 'Saídas dos leitores do tópico de navegação'")
    spark.sql(f"DROP TABLE IF EXISTS {TOPICO}")
    spark.sql(f"""
    CREATE TABLE {TOPICO} (
      seq BIGINT COMMENT 'Posição no tópico',
      chave INT COMMENT 'Chave do evento = cliente_id (mantém a ordem por cliente)',
      evento_id INT, cliente_id INT, ip STRING, pagina STRING, quando TIMESTAMP, publicado_em TIMESTAMP
    ) COMMENT 'Tópico de navegação. Só se anexa no fim; guarda por tempo, e não até ser lido.'
    """)
    spark.sql(f"DELETE FROM {CONTROLE}.offsets WHERE fonte = 'navegacao'")
    spark.sql(f"DROP TABLE IF EXISTS {CONTROLE}.leituras")
    spark.sql(f"""
    CREATE TABLE {CONTROLE}.leituras (
      leitor STRING, rodada INT, iniciada_em TIMESTAMP, terminada_em TIMESTAMP,
      offset_inicial BIGINT, offset_final BIGINT, eventos_lidos BIGINT,
      repetidos BIGINT COMMENT 'Eventos lidos de novo após uma queda e ignorados',
      atraso_maximo BIGINT COMMENT 'Maior número de eventos pendentes encontrado ao acordar',
      caiu BOOLEAN
    ) COMMENT 'Uma linha por rodada de cada leitor do tópico'
    """)
    for tabela, colunas in [("visitas_eventos", "evento_id INT, pagina STRING, contado_em TIMESTAMP"),
                            ("por_cliente_eventos", "evento_id INT, cliente_id INT, contado_em TIMESTAMP")]:
        spark.sql(f"DROP TABLE IF EXISTS {LEITORES}.{tabela}")
        spark.sql(f"CREATE TABLE {LEITORES}.{tabela} ({colunas})")
    spark.sql(f"""CREATE OR REPLACE VIEW {LEITORES}.visitas_por_pagina AS
      SELECT pagina, COUNT(*) AS visitas FROM {LEITORES}.visitas_eventos GROUP BY pagina""")
    spark.sql(f"""CREATE OR REPLACE VIEW {LEITORES}.eventos_por_cliente AS
      SELECT cliente_id, COUNT(*) AS eventos FROM {LEITORES}.por_cliente_eventos GROUP BY cliente_id""")
    spark.sql(f"DELETE FROM {BRONZE}.navegacao")


def eventos_do_dia1():
    return spark.sql(f"SELECT * FROM {ORIGEM}.navegacao ORDER BY evento_id").collect()


def publicar(e) -> int:
    """Anexa um evento ao fim do tópico. Publicar de novo um evento_id já publicado não duplica nada."""
    cliente = "NULL" if e.cliente_id is None else e.cliente_id
    spark.sql(f"""
      INSERT INTO {TOPICO}
      SELECT COALESCE(MAX(seq), 0) + 1, {cliente}, {e.evento_id}, {cliente}, '{e.ip}', '{e.pagina}',
             TIMESTAMP '{e.quando:%Y-%m-%d %H:%M:%S}', current_timestamp()
      FROM {TOPICO}
      HAVING SUM(CASE WHEN evento_id = {e.evento_id} THEN 1 ELSE 0 END) = 0 OR COUNT(*) = 0
    """)
    topo = ultimo_seq()
    print(f"[{agora()}] produtor  · publicou evento {e.evento_id} · {e.pagina} · tópico no seq {topo}")
    return topo

# COMMAND ----------

# MAGIC %md
# MAGIC ## O que cada leitor faz com um trecho do tópico
# MAGIC Cada um devolve (eventos lidos, repetidos ignorados). Todos são idempotentes pelo `evento_id`.

# COMMAND ----------

def _repetidos(tabela: str) -> int:
    return spark.sql(f"""SELECT COUNT(*) AS r FROM _trecho t
                         WHERE EXISTS (SELECT 1 FROM {tabela} x WHERE x.evento_id = t.evento_id)""").first().r


def processar_visitas(trecho, rodada):
    trecho.createOrReplaceTempView("_trecho")
    n, rep = trecho.count(), _repetidos(f"{LEITORES}.visitas_eventos")
    spark.sql(f"""
      MERGE INTO {LEITORES}.visitas_eventos v USING (SELECT evento_id, pagina FROM _trecho) n
      ON v.evento_id = n.evento_id
      WHEN NOT MATCHED THEN INSERT (evento_id, pagina, contado_em) VALUES (n.evento_id, n.pagina, current_timestamp())
    """)
    return n, rep


COLUNAS_NAV = ["evento_id", "cliente_id", "ip", "pagina", "quando"]


def processar_bronze(trecho, rodada):
    trecho.createOrReplaceTempView("_trecho")
    n, rep = trecho.count(), _repetidos(f"{BRONZE}.navegacao")
    spark.sql(f"""
      MERGE INTO {BRONZE}.navegacao b
      USING (SELECT {", ".join(COLUNAS_NAV)}, current_timestamp() AS _carregado_em, {rodada} AS _execucao FROM _trecho) n
      ON b.evento_id = n.evento_id
      WHEN NOT MATCHED THEN INSERT ({", ".join(COLUNAS_NAV)}, _carregado_em, _execucao)
        VALUES ({", ".join("n." + c for c in COLUNAS_NAV)}, n._carregado_em, n._execucao)
    """)
    return n, rep


def processar_por_cliente(trecho, rodada):
    trecho.createOrReplaceTempView("_trecho")
    n, rep = trecho.count(), _repetidos(f"{LEITORES}.por_cliente_eventos")
    spark.sql(f"""
      MERGE INTO {LEITORES}.por_cliente_eventos p USING (SELECT evento_id, cliente_id FROM _trecho) n
      ON p.evento_id = n.evento_id
      WHEN NOT MATCHED THEN INSERT (evento_id, cliente_id, contado_em) VALUES (n.evento_id, n.cliente_id, current_timestamp())
    """)
    return n, rep

# COMMAND ----------

# MAGIC %md
# MAGIC ## O leitor
# MAGIC Cada vez que **acorda**, o leitor lê o que chegou depois do seu offset, processa e só então salva o offset.

# COMMAND ----------

class QuedaSimulada(Exception):
    pass


class Leitor:
    def __init__(self, nome: str, processar):
        self.nome, self.processar = nome, processar
        self.rodada = spark.sql(f"SELECT COALESCE(MAX(rodada), 0) + 1 AS n FROM {CONTROLE}.leituras "
                                f"WHERE leitor = '{nome}'").first().n
        self.inicio = datetime.now()
        self.offset_inicial = self.offset = ler_offset(nome) or 0
        self.lidos = self.repetidos = self.atraso_max = 0
        self.caiu = False
        print(f"[{agora()}] {nome:<9} · rodada {self.rodada} · começa no offset {self.offset}")

    def acordar(self, cair_no_seq: int = 0) -> int:
        """Uma leitura. Devolve quantos eventos leu. Com cair_no_seq > 0, processa até esse seq e cai
        ANTES de salvar o offset (levanta QuedaSimulada)."""
        topo = ultimo_seq()
        pendentes = topo - self.offset
        self.atraso_max = max(self.atraso_max, pendentes)
        if pendentes <= 0:
            return 0
        vai_cair = bool(cair_no_seq) and self.offset < cair_no_seq <= topo
        ate = cair_no_seq if vai_cair else topo
        trecho = spark.table(TOPICO).where(f"seq > {self.offset} AND seq <= {ate}")
        n, rep = self.processar(trecho, self.rodada)
        self.lidos += n
        self.repetidos += rep
        extra = f" · {rep} repetido(s) ignorado(s)" if rep else ""
        if vai_cair:
            self.caiu = True
            print(f"[{agora()}] {self.nome:<9} · processou seq {self.offset + 1}..{ate}{extra} e CAIU antes de "
                  f"salvar o offset (o offset salvo continua {self.offset})")
            raise QuedaSimulada(f"Queda simulada do leitor {self.nome} no seq {ate}")
        print(f"[{agora()}] {self.nome:<9} · encontrou {pendentes} pendente(s) · leu seq {self.offset + 1}..{ate}"
              f"{extra} · offset salvo {ate}")
        self.offset = ate
        gravar_offset(self.nome, self.offset)
        return n

    def encerrar(self) -> None:
        spark.sql(f"""
          INSERT INTO {CONTROLE}.leituras VALUES (
            '{self.nome}', {self.rodada}, TIMESTAMP '{self.inicio:%Y-%m-%d %H:%M:%S}', current_timestamp(),
            {self.offset_inicial}, {self.offset}, {self.lidos}, {self.repetidos}, {self.atraso_max},
            {str(self.caiu).lower()})
        """)


def rodar_leitor(nome: str, processar, intervalo: float, ocioso_max: float = 20,
                 espera_inicial: float = 600, cair_no_seq: int = 0) -> None:
    """Laço de um leitor em notebook próprio: acorda a cada `intervalo` segundos e para sozinho depois de
    `ocioso_max` segundos sem eventos novos."""
    leitor = Leitor(nome, processar)
    t0 = ultimo_novo = time.time()
    viu = False
    try:
        while True:
            if leitor.acordar(cair_no_seq):
                viu, ultimo_novo = True, time.time()
            if viu and time.time() - ultimo_novo > ocioso_max:
                print(f"[{agora()}] {nome} · {ocioso_max:.0f} s sem eventos novos, encerrando")
                break
            if not viu and time.time() - t0 > espera_inicial:
                print(f"[{agora()}] {nome} · nenhum evento em {espera_inicial:.0f} s, encerrando")
                break
            time.sleep(intervalo)
    except QuedaSimulada as q:
        raise RuntimeError(f"{q}. Mude 'Cair no evento' para 0 e rode de novo para religar.") from None
    finally:
        leitor.encerrar()
