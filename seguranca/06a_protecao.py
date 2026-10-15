# Databricks notebook source
# MAGIC %md
# MAGIC # Missão 6 · Funções de proteção
# MAGIC
# MAGIC Biblioteca usada pelo validador (`qualidade/04b_validar`) e pela verificação (`06d`). Não grava nada.
# MAGIC
# MAGIC * **Pseudônimo:** `cliente_ps = HMAC-SHA-256(chave, id do cliente)`, em hexadecimal. Mesma chave e mesmo id dão
# MAGIC   sempre o mesmo pseudônimo, então `clientes`, `pedidos` e `navegacao` continuam se ligando na silver. Sem a
# MAGIC   chave, não dá para voltar ao id nem testar ids um a um (diferente de um hash simples, que qualquer um recalcula).
# MAGIC * **Chave `CHAVE_PSEUDONIMO`:** nunca no código nem no repositório. Procurada, nesta ordem, na variável de
# MAGIC   ambiente `CHAVE_PSEUDONIMO`, no secret scope `lab_bd` do Databricks e, se a conta não permitir secret scopes,
# MAGIC   num arquivo na pasta pessoal do usuário (fora do Git folder). É criada por `seguranca/06b_criar_chave`.
# MAGIC * **Máscara:** troca letras e dígitos por `*`, menos os dois últimos caracteres. Usada na quarentena, que guarda a
# MAGIC   linha reprovada sem o dado pessoal em claro.

# COMMAND ----------

import hmac
import hashlib
import os

ESCOPO_SEGREDO = "lab_bd"
NOME_CHAVE = "CHAVE_PSEUDONIMO"


def _usuario_atual() -> str:
    return spark.sql("SELECT current_user() AS u").first().u


def caminho_chave_reserva() -> str:
    """Arquivo de reserva, na pasta pessoal do usuário: fora do Git folder, portanto fora do repositório."""
    return f"/Workspace/Users/{_usuario_atual()}/lab_bd_segredos/{NOME_CHAVE}"


def onde_esta_a_chave():
    """Devolve (origem, chave) ou (None, None). A chave nunca é impressa."""
    if os.environ.get(NOME_CHAVE):
        return "variável de ambiente", os.environ[NOME_CHAVE]
    try:
        return f"secret scope {ESCOPO_SEGREDO}", dbutils.secrets.get(ESCOPO_SEGREDO, NOME_CHAVE)
    except Exception:
        pass
    try:
        with open(caminho_chave_reserva(), encoding="utf-8") as f:
            return "arquivo na pasta pessoal (fora do repositório)", f.read().strip()
    except Exception:
        return None, None


def obter_chave() -> str:
    origem, chave = onde_esta_a_chave()
    if not chave:
        raise RuntimeError(f"A chave {NOME_CHAVE} não foi encontrada. Rodem seguranca/06b_criar_chave antes "
                           f"do validador. A chave fica fora do código e do repositório.")
    return chave


def pseudonimo_de(valor, chave: str):
    if valor is None or str(valor).strip() == "":
        return None
    return hmac.new(chave.encode("utf-8"), str(valor).strip().encode("utf-8"), hashlib.sha256).hexdigest()


def registrar_pseudonimo(chave: str = None) -> str:
    """Registra a função SQL `pseudonimo(texto)`. A chave vai na closure da função, nunca no texto do SQL."""
    k = chave or obter_chave()
    spark.udf.register("pseudonimo", lambda v: pseudonimo_de(v, k), "string")
    return "informada" if chave else onde_esta_a_chave()[0]


def sql_mascara(expr: str) -> str:
    """Expressão SQL que mascara um texto: letras e dígitos viram *, menos os dois últimos caracteres."""
    return (f"CASE WHEN {expr} IS NULL OR length({expr}) <= 2 THEN {expr} ELSE "
            f"concat(regexp_replace(substr({expr}, 1, length({expr}) - 2), '[0-9A-Za-zÀ-ÿ]', '*'), "
            f"substr({expr}, -2)) END")
