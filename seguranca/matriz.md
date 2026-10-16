# Matriz de proteção por classe

Cada classe do acordo de governança (`governanca/acordo.yaml`) e dos contratos (`contratos/*.yaml`) exige uma
proteção. A classe de cada campo está no contrato; o catálogo (`governanca/catalogo.md`) mostra o que acontece com
ele na silver. A regra como código (`governanca/05a_governanca`) para o pipeline se um campo pessoal não declarar a
sua proteção.

| Proteção | público | interno | confidencial | pessoal | pessoal sensível |
|---|---|---|---|---|---|
| Onde aparece em claro | em qualquer camada | em qualquer camada | em qualquer camada, só para quem tem acesso | só na origem, no log e na bronze | não usado neste laboratório: não entraria nem na bronze sem decisão do dono e do jurídico |
| Na silver | mantido | mantido | mantido | **pseudônimo** (`cliente_ps`) ou **removido**; quase-identificador fica só com finalidade registrada (Missão 7) | removido |
| Na gold | mantido | mantido | agregado | só agregado, nunca por pessoa | nunca |
| Na quarentena | mantido | mantido | mantido | **máscara** (removido) ou pseudônimo | nunca |
| Quem lê | qualquer um | qualquer um do time de dados | papel com aprovação do dono | bronze: só o papel de pipeline; o analista lê a silver e a gold | ninguém sem aprovação do dono e do jurídico |
| Chaves e segredos | — | — | — | chave do pseudônimo fora do código e do repositório (secret scope) | chave separada, com rotação |
| Logs e evidências | pode aparecer | pode aparecer | sem valores individuais | **nunca** o valor: só onde está e quantas linhas | nunca |
| Retenção | do conjunto | do conjunto | do conjunto | prazo obrigatório no acordo (regra como código) | prazo obrigatório e menor possível |

## Como cada campo pessoal foi protegido

| Conjunto | Campo | Na silver | Na quarentena |
|---|---|---|---|
| clientes | `id` | pseudônimo `cliente_ps` | pseudônimo |
| clientes | `nome`, `email`, `cpf` | removidos | máscara |
| clientes | `nascimento`, `cidade` | mantidos (quase-identificadores; a Missão 7 registra a finalidade e generaliza o nascimento) | em claro |
| pedidos | `cliente_id` | pseudônimo `cliente_ps` | pseudônimo |
| pedidos | `observacao` | removida (achada pelo varredor: e-mail no 104, telefone no 106) | máscara |
| navegacao | `cliente_id` | pseudônimo `cliente_ps` | pseudônimo |
| navegacao | `ip` | removido (achado pelo varredor) | máscara |

`valor_centavos` (confidencial) fica na silver: só o papel de analista lê a silver, e a gold mostra só agregados.

## Pseudônimo

`cliente_ps = HMAC-SHA-256(CHAVE_PSEUDONIMO, id do cliente)`, em hexadecimal (64 caracteres).

- **Por que HMAC e não um hash simples:** `SHA-256(id)` qualquer pessoa recalcula para os ids 1, 2, 3… e descobre
  quem é quem. Com HMAC, sem a chave não dá para recalcular.
- **Mesmo pseudônimo nas três tabelas:** a mesma chave e o mesmo id dão sempre o mesmo resultado, então `clientes`,
  `pedidos` e `navegacao` continuam se ligando na silver, sem o id real.
- **Onde fica a chave:** no secret scope `lab_bd` do Databricks, criada por `seguranca/06b_criar_chave` com 256 bits
  aleatórios. Nunca no código, no repositório, no log ou na evidência; o Databricks mostra `[REDACTED]` se alguém
  tentar imprimir. Se a conta não permitir secret scopes, ela vai para um arquivo na pasta pessoal do usuário, fora do
  Git folder.
- **Trocar a chave** muda todos os pseudônimos: é preciso refazer a silver. Perder a chave não perde dado, só a
  ligação com pseudônimos antigos que estiverem fora da silver.
- **Pseudônimo não é anônimo:** quem tem a chave e a bronze volta à pessoa, e a combinação de cidade e nascimento
  pode identificar alguém. Isso é a Missão 7.

## Papéis

| Papel | Principal | Lê | Não lê |
|---|---|---|---|
| Pipeline | grupo `lab_pipeline` ou, na Free Edition, o dono dos schemas | origem, log, bronze, controle, qualidade, silver, gold | |
| Analista | grupo `lab_analistas` ou, na Free Edition, `account users` | silver e gold | origem, log, bronze, controle, quarentena e varredura |

As permissões são código (`seguranca/06c_papeis`): `GRANT` para o que cada papel pode e `REVOKE` do analista em
tudo que tem dado pessoal em claro. O notebook mostra as permissões que ficaram valendo e para se o analista ainda
conseguir ler a bronze.
