# Databricks notebook source
# MAGIC %md
# MAGIC # Missão 5 · Regra como código
# MAGIC
# MAGIC Este notebook lê `governanca/acordo.yaml` e os contratos em `contratos/*.yaml` e **trava o pipeline** quando
# MAGIC o acordo não é cumprido. O validador (`qualidade/04b_validar`) roda este notebook na primeira célula: se uma
# MAGIC regra falhar, nada segue para a silver.
# MAGIC
# MAGIC Regras verificadas:
# MAGIC 1. Todo conjunto de classe **pessoal** ou **pessoal sensível** tem prazo de retenção (`retencao.prazo`).
# MAGIC 2. Todo conjunto com contrato tem entrada no acordo, com dono que é **um time** (não uma pessoa).
# MAGIC 3. Todo campo do contrato tem classe, e a classe do conjunto é a do **campo mais sensível**.
# MAGIC 4. A classe do conjunto no acordo é a mesma do contrato.
# MAGIC 5. Todo campo pessoal declara como é protegido na silver (`protecao`: pseudonimo, remover ou manter). (Missão 6)
# MAGIC
# MAGIC Também ficam aqui as funções que o gerador do catálogo (`05b`) e a verificação (`05d`) usam.
# MAGIC Rodado sozinho, ele só confere as regras e mostra o resultado.

# COMMAND ----------

import os
import copy

try:
    import yaml
except ImportError:
    raise ImportError("O pacote PyYAML não está instalado nesta computação. Rodem numa célula: %pip install pyyaml")

# Os notebooks que chamam este (qualidade/ e governanca/) ficam um nível abaixo da raiz do repositório.
RAIZ_REPO = os.path.abspath(os.path.join(os.getcwd(), ".."))
CLASSES = ["público", "interno", "confidencial", "pessoal", "pessoal sensível"]
CLASSES_COM_RETENCAO = {"pessoal", "pessoal sensível"}
PROTECOES = ["pseudonimo", "remover", "manter"]
# Padrões de dado pessoal (expressões regulares Java, usadas pelo rlike do Spark): varredor (05c) e Missão 6
PADROES_PESSOAIS = {
    "e-mail": r"[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(\.[A-Za-z0-9-]+)+",
    "telefone": r"\(?\b\d{2}\)?\s?9?\d{4}-\d{4}\b",
    "CPF": r"\b\d{3}\.\d{3}\.\d{3}-\d{2}\b",
    "IP": r"\b(\d{1,3}\.){3}\d{1,3}\b",
}


class GovernancaError(Exception):
    """O acordo de governança não foi cumprido. O pipeline para aqui."""


def ler_yaml(*partes) -> dict:
    with open(os.path.join(RAIZ_REPO, *partes), encoding="utf-8") as f:
        return yaml.safe_load(f)


def ler_contratos() -> dict:
    pasta = os.path.join(RAIZ_REPO, "contratos")
    contratos = {}
    for nome in sorted(os.listdir(pasta)):
        if nome.endswith(".yaml"):
            c = ler_yaml("contratos", nome)
            contratos[c["conjunto"]] = c
    return contratos


def ler_acordo() -> dict:
    return ler_yaml("governanca", "acordo.yaml")


def conjuntos_do_acordo(acordo: dict) -> dict:
    return {c["nome"]: c for c in (acordo or {}).get("conjuntos", [])}


def classe_mais_sensivel(classes):
    validas = [c for c in classes if c in CLASSES]
    return max(validas, key=CLASSES.index) if validas else None


def verificar_regras(contratos: dict, acordo: dict) -> list:
    """Devolve a lista de violações (vazia quando o acordo é cumprido). Não lança exceção."""
    erros = []
    no_acordo = conjuntos_do_acordo(acordo)

    for nome, contrato in contratos.items():
        campos = contrato.get("campos", [])
        sem_classe = [c["nome"] for c in campos if not c.get("classe")]
        invalidas = [f"{c['nome']} ({c['classe']})" for c in campos if c.get("classe") and c["classe"] not in CLASSES]
        if sem_classe:
            erros.append(f"contrato '{nome}': campo(s) sem classe: {', '.join(sem_classe)}. "
                         f"Declarem a classe de cada campo em contratos/{nome}.yaml.")
        if invalidas:
            erros.append(f"contrato '{nome}': classe desconhecida em {', '.join(invalidas)}. "
                         f"Classes aceitas: {', '.join(CLASSES)}.")
        sem_protecao = [c["nome"] for c in campos if c.get("classe") in CLASSES_COM_RETENCAO
                        and c.get("protecao") not in PROTECOES]
        if sem_protecao:
            erros.append(f"contrato '{nome}': campo(s) pessoal(is) sem proteção para a silver: "
                         f"{', '.join(sem_protecao)}. Declarem protecao: {' | '.join(PROTECOES)}.")
        if sum(1 for c in campos if c.get("protecao") == "pseudonimo") > 1:
            erros.append(f"contrato '{nome}': só o campo que identifica o cliente vira pseudônimo (cliente_ps).")
        esperada = classe_mais_sensivel(c.get("classe") for c in campos)
        if esperada and contrato.get("classe") != esperada:
            erros.append(f"contrato '{nome}': a classe do conjunto é '{contrato.get('classe')}', mas o campo mais "
                         f"sensível é '{esperada}'. A classe do conjunto tem de ser a do campo mais sensível.")

        entrada = no_acordo.get(nome)
        if entrada is None:
            erros.append(f"conjunto '{nome}' tem contrato mas não está em governanca/acordo.yaml. "
                         f"Todo conjunto precisa de dono, classe, retenção e acesso.")
            continue
        dono = str(entrada.get("dono") or "").strip()
        if not dono.lower().startswith("time"):
            erros.append(f"conjunto '{nome}': o dono precisa ser um time (ex.: 'time de Vendas'), não "
                         f"'{dono or 'vazio'}'. Pessoa sai da empresa; time continua.")
        if esperada and entrada.get("classe") != esperada:
            erros.append(f"conjunto '{nome}': o acordo diz classe '{entrada.get('classe')}', o contrato diz "
                         f"'{esperada}'. Acordo e contrato têm de concordar.")

    for nome, entrada in no_acordo.items():
        classe = entrada.get("classe")
        prazo = str(((entrada.get("retencao") or {}).get("prazo")) or "").strip()
        if classe in CLASSES_COM_RETENCAO and not prazo:
            erros.append(f"conjunto '{nome}' é de classe '{classe}' e não tem prazo de retenção "
                         f"(retencao.prazo em governanca/acordo.yaml). Dado pessoal sem prazo para sair "
                         f"não pode seguir no pipeline: definam o prazo e rodem de novo.")
        if not (entrada.get("acesso") or {}).get("quem_aprova"):
            erros.append(f"conjunto '{nome}': falta acesso.quem_aprova no acordo.")
    return erros


def checar_governanca(contratos=None, acordo=None) -> None:
    """Lança GovernancaError com todas as violações. É isto que trava o pipeline."""
    contratos = ler_contratos() if contratos is None else contratos
    acordo = ler_acordo() if acordo is None else acordo
    erros = verificar_regras(contratos, acordo)
    if erros:
        raise GovernancaError("O pipeline parou: o acordo de governança não foi cumprido.\n  - "
                              + "\n  - ".join(erros))
    print(f"Governança OK: {len(contratos)} contratos e {len(conjuntos_do_acordo(acordo))} conjuntos no acordo "
          f"cumprem as regras (dono, classe, retenção e acesso).")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Catálogo gerado
# MAGIC
# MAGIC `gerar_catalogo` junta o contrato (descrição, campos, classes, frescor) com o acordo (dono, retenção,
# MAGIC acesso) e com o que a plataforma sabe da tabela (linhas, última alteração). Ninguém edita o catálogo à mão.

# COMMAND ----------

def _txt(v) -> str:
    return " ".join(str(v).split()) if v is not None else "—"


def gerar_catalogo(contratos: dict, acordo: dict, metadados: dict = None, gerado_em: str = None) -> str:
    no_acordo = conjuntos_do_acordo(acordo)
    metadados = metadados or {}
    linhas = ["# Catálogo de dados · Laboratório BD", "",
              "> Gerado por script (`governanca/05b_gerar_catalogo`) a partir de `contratos/*.yaml` e de",
              "> `governanca/acordo.yaml`. **Não editem este arquivo à mão**: mudem o contrato ou o acordo e gerem de novo.",
              ""]
    if gerado_em:
        linhas += [f"_Gerado em {gerado_em}._", ""]
    linhas += ["| Conjunto | Classe | Dono | Retenção | Frescor |", "|---|---|---|---|---|"]
    for nome, c in contratos.items():
        a = no_acordo.get(nome, {})
        linhas.append(f"| [{nome}](#{nome}) | {c.get('classe')} | {_txt(a.get('dono'))} | "
                      f"{_txt((a.get('retencao') or {}).get('prazo'))} | {_txt(c.get('frescor'))} |")
    for nome, c in contratos.items():
        a = no_acordo.get(nome, {})
        ret, ace = a.get("retencao") or {}, a.get("acesso") or {}
        linhas += ["", f"## {nome}", "", _txt(c.get("descricao")), "",
                   f"- **Dono:** {_txt(a.get('dono'))}",
                   f"- **Classe:** {c.get('classe')} (a do campo mais sensível)",
                   f"- **Frescor prometido:** {_txt(c.get('frescor'))}",
                   f"- **Retenção:** {_txt(ret.get('prazo'))}. {_txt(ret.get('motivo'))}",
                   f"- **Acesso:** pedem {_txt(ace.get('quem_pede'))}; aprova {_txt(ace.get('quem_aprova'))}; "
                   f"auditoria: {_txt(ace.get('auditoria'))}",
                   f"- **Contrato:** `contratos/{nome}.yaml`, versão {c.get('versao')}"]
        for camada, m in sorted(metadados.get(nome, {}).items()):
            linhas.append(f"- **Tabela {camada}:** `{m['tabela']}` · {m['linhas']} linhas · "
                          f"última alteração {m['alterada_em']}")
        linhas += ["", "| Campo | Tipo | Classe | Na silver | Regras | Descrição |", "|---|---|---|---|---|---|"]
        for f in c.get("campos", []):
            regras = []
            if f.get("obrigatorio"):
                regras.append("obrigatório")
            if f.get("unico"):
                regras.append("único")
            if "minimo" in f:
                regras.append(f"mínimo {f['minimo']}")
            if "maximo" in f:
                regras.append(f"máximo: {f['maximo']}")
            if "dominio" in f:
                regras.append("um de: " + ", ".join(map(str, f["dominio"])))
            classe = f.get("classe", "—") + (" ⚠ achado pelo varredor" if f.get("classe_encontrada_pelo_varredor") else "")
            silver = {"pseudonimo": "pseudônimo `cliente_ps`", "remover": "removido",
                      "manter": "mantido"}.get(f.get("protecao"), "mantido")
            linhas.append(f"| `{f['nome']}` | {f.get('tipo')} | {classe} | {silver} | {', '.join(regras) or '—'} | "
                          f"{_txt(f.get('descricao'))} |")
    linhas += ["", "---", "", "Classes, da menos para a mais sensível: " + " < ".join(CLASSES) + ".",
               "Regra como código (`governanca/05a_governanca`): conjunto de classe pessoal sem prazo de retenção "
               "para o pipeline.",
               "Na silver: pseudônimo = HMAC-SHA-256 do id com a chave CHAVE_PSEUDONIMO (fora do repositório); "
               "proteção exigida por classe em `seguranca/matriz.md`.", ""]
    return "\n".join(linhas)


# COMMAND ----------

# Gate: quem chama este notebook (o validador, o gerador do catálogo) para aqui se o acordo não for cumprido.
checar_governanca()
