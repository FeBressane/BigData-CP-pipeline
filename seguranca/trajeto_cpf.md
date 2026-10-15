# Trajeto do CPF

Por onde o CPF passa, do cadastro até o consumo, e como ele está em cada etapa: **real**, **token** (pseudônimo),
**máscara** ou **ausente**. O estado de cada etapa é medido por `seguranca/06d_verificar_missao6`, que procura o
padrão de CPF em cada lugar, e o resultado está em `evidencias/missao6.txt`.

| # | Etapa | Onde | Estado | Quem lê | Por quê |
|---|---|---|---|---|---|
| 1 | Origem | `lab_origem.clientes.cpf` | real | sistema da loja | é o cadastro |
| 2 | Log de mudanças | `lab_log.mudancas.dados` (JSON) | real | papel `lab_pipeline` | o log copia a linha inteira de cada mudança, inclusive a exclusão |
| 3 | Bronze | `lab_bronze.clientes.cpf` | real | papel `lab_pipeline` | a bronze guarda o dado como chegou |
| 4 | Quarentena | `lab_qualidade.quarentena.dados` | máscara | papel `lab_pipeline` | quem corrige a linha precisa saber qual campo falhou, não o CPF |
| 5 | Silver | `lab_silver.clientes` | ausente | papel `lab_analistas` | nenhuma análise precisa do CPF; o cliente é ligado pelo `cliente_ps` |
| 6 | Gold | `lab_gold` | ausente | papel `lab_analistas` | só agregados |
| 7 | Varredor | `lab_qualidade.varredura` | ausente | papel `lab_pipeline` | guarda onde o CPF está, nunca o valor |
| 8 | Catálogo e evidências | `governanca/catalogo.md`, `evidencias/*.txt` | ausente | qualquer um do repositório | só metadados e contagens |

O CPF nunca vira token: não existe uso que precise ligar registros pelo CPF, então ele sai na silver. Quem liga os
registros é o `cliente_ps`, feito do id.

**Versões antigas.** O Delta guarda versões anteriores das tabelas. Na silver, a versão sem proteção (Missões 4 e 5)
foi apagada: o validador apaga a tabela quando ela ainda tem colunas protegidas e cria de novo, e a verificação lê
todas as versões por time travel. Na origem, no log, na bronze e na quarentena, as versões antigas guardam o CPF em
claro (na quarentena, de antes da máscara) até a limpeza do Delta. Esses lugares entram no inventário de moradas da
Missão 8.
