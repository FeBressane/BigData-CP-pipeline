# Databricks notebook source
# MAGIC %md
# MAGIC # Missão 6 · Criar a chave do pseudônimo
# MAGIC
# MAGIC Gera uma chave aleatória de 256 bits e guarda como `CHAVE_PSEUDONIMO` **fora do código e do repositório**:
# MAGIC no secret scope `lab_bd` do Databricks. Se a conta não permitir secret scopes, guarda num arquivo na pasta
# MAGIC pessoal do usuário (`/Workspace/Users/<você>/lab_bd_segredos/`), que não faz parte do Git folder.
# MAGIC
# MAGIC Rodem **uma vez**. Rodar de novo mantém a chave existente. Trocar a chave (widget = `sim`) muda todos os
# MAGIC pseudônimos: depois disso é preciso rodar o validador de novo para refazer a silver.
# MAGIC
# MAGIC A chave nunca é impressa. O Databricks também esconde (`[REDACTED]`) valores lidos de secret scopes.

# COMMAND ----------

# MAGIC %run ./06a_protecao

# COMMAND ----------

import secrets as _secrets

dbutils.widgets.dropdown("trocar", "nao", ["nao", "sim"], "Trocar a chave existente?")
TROCAR = dbutils.widgets.get("trocar") == "sim"

origem, atual = onde_esta_a_chave()
if atual and not TROCAR:
    print(f"A chave já existe ({origem}). Nada foi alterado.")
else:
    nova = _secrets.token_hex(32)
    guardada = None
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        if ESCOPO_SEGREDO not in [s.name for s in w.secrets.list_scopes()]:
            w.secrets.create_scope(scope=ESCOPO_SEGREDO)
        w.secrets.put_secret(scope=ESCOPO_SEGREDO, key=NOME_CHAVE, string_value=nova)
        guardada = f"secret scope {ESCOPO_SEGREDO}"
    except Exception as e:
        print(f"Secret scope indisponível nesta conta ({type(e).__name__}). Usando o arquivo na pasta pessoal.")
        caminho = caminho_chave_reserva()
        os.makedirs(os.path.dirname(caminho), exist_ok=True)
        with open(caminho, "w", encoding="utf-8") as f:
            f.write(nova)
        guardada = f"arquivo {caminho} (fora do repositório)"
    del nova
    print(f"Chave {'trocada' if atual else 'criada'} e guardada em: {guardada}.")
    if atual:
        print("Todos os pseudônimos mudaram. Rodem qualidade/04b_validar de novo para refazer a silver.")

origem, chave = onde_esta_a_chave()
assert chave and len(chave) == 64, "A chave não pôde ser lida de volta."
print(f"Conferido: a chave é lida de volta a partir de: {origem}. Tamanho: 256 bits.")
del chave
