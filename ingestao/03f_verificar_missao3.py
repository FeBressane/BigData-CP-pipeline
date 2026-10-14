# Databricks notebook source
# MAGIC %md
# MAGIC # Missão 3 · Verificação e evidência
# MAGIC
# MAGIC Confere as cinco verificações do laboratório e grava `evidencias/missao3.txt` com as saídas dos três
# MAGIC leitores e o histórico das rodadas.

# COMMAND ----------

# MAGIC %run ./03_comum

# COMMAND ----------

import os

visitas = {r.pagina: r.visitas for r in spark.table(f"{LEITORES}.visitas_por_pagina").collect()}
por_cliente = {r.cliente_id: r.eventos for r in spark.table(f"{LEITORES}.eventos_por_cliente").collect()}
b = spark.sql(f"SELECT COUNT(*) AS n, COUNT(DISTINCT evento_id) AS d FROM {BRONZE}.navegacao").first()
topico = spark.sql(f"SELECT COUNT(*) AS n FROM {TOPICO}").first().n
rodadas = spark.sql(f"SELECT * FROM {CONTROLE}.leituras ORDER BY iniciada_em").collect()
display(spark.sql(f"SELECT * FROM {CONTROLE}.leituras ORDER BY iniciada_em"))

ult = lambda nome: [r for r in rodadas if r.leitor == nome]
r_vis, r_bro, r_cli = ult("visitas"), ult("bronze"), ult("por_cliente")
# o atraso que importa é o da primeira rodada de cada um, que rodou durante a publicação
atraso_vis = r_vis[0].atraso_maximo if r_vis else None
atraso_bro = r_bro[0].atraso_maximo if r_bro else None
queda = next((r for r in r_vis if r.caiu), None)
religou = next((r for r in r_vis if not r.caiu and queda and r.iniciada_em > queda.iniciada_em), None)

esperado_visitas = {"/carrinho": 2, "/produto/tenis": 1, "/produto/mochila": 1, "/produto/relogio": 1,
                    "/produto/fone": 1, "/home": 1, "/produto/camiseta": 1}
esperado_cliente = {1: 2, 5: 2, 2: 1, 3: 1, 6: 1, None: 1}

checks = [
    ("Durante a publicação, o leitor rápido fica no último evento e a bronze até 5 eventos atrás",
     atraso_vis is not None and atraso_bro is not None and atraso_vis < atraso_bro <= 6,
     f"maior número de eventos pendentes ao acordar: visitas {atraso_vis}, bronze {atraso_bro}"),
    ("visitas: /carrinho com 2 visitas e as outras seis páginas com 1; bronze com os mesmos 8 eventos, sem duplicata",
     visitas == esperado_visitas and (b.n, b.d) == (8, 8),
     f"visitas {dict(sorted(visitas.items()))} · bronze {b.n} eventos ({b.d} distintos)"),
    ("Derrubar visitas depois do 4º evento e religar: retoma do offset salvo, mesmos totais, nada contado duas vezes",
     queda is not None and religou is not None and sum(visitas.values()) == 8,
     (f"caiu na rodada {queda.rodada} com o offset salvo em {queda.offset_final}; religou na rodada {religou.rodada} "
      f"do offset {religou.offset_inicial} ao {religou.offset_final}, {religou.repetidos} repetido(s) ignorado(s); "
      f"total contado {sum(visitas.values())}") if queda and religou else "rodem o visitas com 'Cair no evento' = 4 e depois com 0"),
    ("Leitor novo: clientes 1 e 5 com 2 eventos, 2, 3 e 6 com 1, e 1 visitante sem cadastro",
     por_cliente == esperado_cliente and bool(r_cli) and r_cli[0].offset_inicial == 0,
     f"{ {('visitante' if k is None else k): v for k, v in por_cliente.items()} } · começou no offset "
     f"{r_cli[0].offset_inicial if r_cli else '-'}"),
    ("Depois de todos lerem, o log continua com os 8 eventos", topico == 8, f"tópico com {topico} eventos"),
]

linhas = [f"Missão 3 · um log, vários leitores (Trilha A, tópico Delta) · verificado em {datetime.now():%Y-%m-%d %H:%M}", ""]
for i, (desc, ok, det) in enumerate(checks, 1):
    linhas += [f"[{'OK' if ok else 'PENDENTE'}] {i}. {desc}", f"      {det}"]

linhas += ["", "Saída do leitor visitas (lab_leitores.visitas_por_pagina):"]
linhas += [f"  {p:<20} {v}" for p, v in sorted(visitas.items(), key=lambda x: (-x[1], x[0]))]
linhas += ["", "Saída do leitor por_cliente (lab_leitores.eventos_por_cliente):"]
linhas += [f"  {('visitante sem cadastro' if k is None else 'cliente ' + str(k)):<24} {v}"
           for k, v in sorted(por_cliente.items(), key=lambda x: (x[0] is None, x[0] or 0))]
linhas += ["", f"Saída do leitor bronze: {b.n} eventos em lab_bronze.navegacao", "",
           "Rodadas dos leitores (lab_controle.leituras):"]
for r in rodadas:
    linhas.append(f"  {r.leitor:<12} rodada {r.rodada}: offset {r.offset_inicial} -> {r.offset_final}, "
                  f"leu {r.eventos_lidos}, repetidos {r.repetidos}, pendentes ao acordar até {r.atraso_maximo}"
                  + (" · CAIU antes de salvar o offset" if r.caiu else ""))
linhas += ["", "O IP dos eventos não entra nesta evidência: é dado pessoal."]

texto = "\n".join(linhas)
print(texto)
caminho = os.path.abspath(os.path.join(os.getcwd(), "..", "evidencias", "missao3.txt"))
with open(caminho, "w", encoding="utf-8") as f:
    f.write(texto + "\n")
print("\nEvidência gravada em", caminho)
