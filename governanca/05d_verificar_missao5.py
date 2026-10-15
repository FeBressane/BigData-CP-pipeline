# Databricks notebook source
# MAGIC %md
# MAGIC # Missão 5 · Verificação e evidência
# MAGIC
# MAGIC Confere as três verificações do laboratório e grava `evidencias/missao5.txt`:
# MAGIC 1. Sem retenção em `navegacao`, a regra como código para o pipeline com mensagem clara; com o prazo de
# MAGIC    volta, passa. (O teste tira o prazo de uma cópia do acordo em memória; o arquivo não é alterado.)
# MAGIC 2. O varredor aponta a `observacao` dos pedidos 104 e 106 e a coluna `ip` nos 8 eventos.
# MAGIC 3. Mudar uma descrição no contrato e regerar: o catálogo reflete a mudança sem edição manual.
# MAGIC
# MAGIC Rodem antes `05b_gerar_catalogo` e `05c_varredor`. A evidência não traz nenhum valor pessoal, só onde ele está.

# COMMAND ----------

# MAGIC %run ./05a_governanca

# COMMAND ----------

from datetime import datetime

dbutils.widgets.text("catalogo", "workspace", "Catálogo")
CATALOGO = dbutils.widgets.get("catalogo")
QUALIDADE = f"{CATALOGO}.lab_qualidade"

contratos, acordo = ler_contratos(), ler_acordo()

# 1. Regra como código: tirar o prazo da navegação, checar, devolver o prazo, checar de novo
sem_prazo = copy.deepcopy(acordo)
for c in sem_prazo["conjuntos"]:
    if c["nome"] == "navegacao":
        c["retencao"].pop("prazo", None)
try:
    checar_governanca(contratos, sem_prazo)
    msg_parou = None
except GovernancaError as e:
    msg_parou = str(e)
erros_com_prazo = verificar_regras(contratos, acordo)
regra_ok = (msg_parou is not None and "'navegacao'" in msg_parou and "retenção" in msg_parou
            and not erros_com_prazo)

# 2. Varredor
achados = spark.table(f"{QUALIDADE}.varredura").collect()
def achado(tabela, coluna, padrao):
    return next((a for a in achados if a.schema == "lab_bronze" and a.tabela == tabela
                 and a.coluna == coluna and a.padrao == padrao), None)
obs_email, obs_fone = achado("pedidos", "observacao", "e-mail"), achado("pedidos", "observacao", "telefone")
ip = achado("navegacao", "ip", "IP")
chaves_obs = sorted({k for a in (obs_email, obs_fone) if a for k in a.chaves.split(", ")})
varredor_ok = (chaves_obs == ["104", "106"] and ip is not None and ip.linhas == 8)
nao_declarados = [a for a in achados if a.situacao.startswith("NÃO DECLARADO")]

# 3. Catálogo: mudar a descrição em memória e regerar
descricao_nova = "Pedidos da loja (descrição alterada pelo teste da Missão 5)."
alterados = copy.deepcopy(contratos)
alterados["pedidos"]["descricao"] = descricao_nova
antes, depois = gerar_catalogo(contratos, acordo), gerar_catalogo(alterados, acordo)
catalogo_ok_teste = (descricao_nova in depois and descricao_nova not in antes
                     and contratos["pedidos"]["descricao"] in antes)
caminho_catalogo = os.path.join(RAIZ_REPO, "governanca", "catalogo.md")
catalogo_arquivo = open(caminho_catalogo, encoding="utf-8").read() if os.path.exists(caminho_catalogo) else ""
catalogo_em_dia = bool(catalogo_arquivo) and "Gerado por script" in catalogo_arquivo and all(
    " ".join(c["descricao"].split()) in catalogo_arquivo for c in contratos.values())

checks = [
    ("Sem retenção em navegacao o pipeline para com mensagem clara; com o prazo de volta, passa",
     regra_ok,
     ("sem o prazo: " + (msg_parou.splitlines()[-1].strip(" -") if msg_parou else "NÃO PAROU")
      + f" · com o prazo: {len(erros_com_prazo)} violação(ões)")),
    ("O varredor aponta a observacao dos pedidos 104 e 106 e a coluna ip nos 8 eventos",
     varredor_ok,
     f"observacao: e-mail em {obs_email.chaves if obs_email else '—'}, telefone em {obs_fone.chaves if obs_fone else '—'}"
     f" · ip: {ip.linhas if ip else 0} eventos"),
    ("Mudar uma descrição no contrato e regerar: o catálogo reflete sem edição manual",
     catalogo_ok_teste and catalogo_em_dia,
     f"descrição nova aparece no catálogo regerado: {'sim' if catalogo_ok_teste else 'não'} · "
     f"governanca/catalogo.md gerado e em dia com os contratos: {'sim' if catalogo_em_dia else 'não (rodem 05b)'}"),
]

no_acordo = conjuntos_do_acordo(acordo)
linhas = [f"Missão 5 · governança, catálogo e varredor · verificado em {datetime.now():%Y-%m-%d %H:%M}", ""]
for i, (desc, ok, det) in enumerate(checks, 1):
    linhas += [f"[{'OK' if ok else 'PENDENTE'}] {i}. {desc}", f"      {det}"]

linhas += ["", "Acordo de governança (governanca/acordo.yaml):"]
for nome, a in no_acordo.items():
    linhas.append(f"  {nome:<10} classe {a['classe']:<9} · dono: {a['dono']} · retenção: {a['retencao']['prazo']}")

linhas += ["", "Mensagem da regra como código quando falta o prazo da navegação:"]
linhas += ["  " + l for l in (msg_parou or "(não parou)").splitlines()]

linhas += ["", "Saída do varredor (lab_qualidade.varredura; só onde, nunca o valor):",
           "  tabela                     coluna      padrão    linhas  chaves                situação"]
for a in sorted(achados, key=lambda a: (a.schema, a.tabela, a.coluna, a.padrao)):
    chaves = a.chaves if len(a.chaves) <= 20 else a.chaves[:17] + "..."
    linhas.append(f"  {a.schema + '.' + a.tabela:<26} {a.coluna:<11} {a.padrao:<9} {a.linhas:>6}  "
                  f"{(a.chave or '') + ' ' + chaves:<21} {a.situacao}")
linhas += ["", f"Colunas com dado pessoal não declarado no contrato: {len(nao_declarados)}",
           "O produtor não tinha declarado a observacao dos pedidos nem o ip da navegação como pessoais. O varredor",
           "achou os dois; os contratos foram corrigidos (classe pessoal, classe_encontrada_pelo_varredor: true).",
           "O log de mudanças (lab_log.mudancas) e o tópico (lab_log.navegacao) também guardam esses dados: são",
           "outras moradas e entram no inventário da Missão 8."]

texto = "\n".join(linhas)
print(texto)
caminho = os.path.join(RAIZ_REPO, "evidencias", "missao5.txt")
with open(caminho, "w", encoding="utf-8") as f:
    f.write(texto + "\n")
print("\nEvidência gravada em", caminho)
