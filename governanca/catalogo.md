# Catálogo de dados · Laboratório BD

> Gerado por script (`governanca/05b_gerar_catalogo`) a partir de `contratos/*.yaml` e de
> `governanca/acordo.yaml`. **Não editem este arquivo à mão**: mudem o contrato ou o acordo e gerem de novo.

_Gerado em 2026-10-15 23:49._

| Conjunto | Classe | Dono | Retenção | Frescor |
|---|---|---|---|---|
| [clientes](#clientes) | pessoal | time de Cadastro e Atendimento | enquanto o cliente tiver conta ativa | até 24 horas depois da mudança na origem |
| [navegacao](#navegacao) | pessoal | time do Site | 90 dias | até 5 segundos depois da publicação no tópico |
| [pedidos](#pedidos) | pessoal | time de Vendas | 5 anos após o fim do ano do pedido | até 24 horas depois da mudança na origem |

## clientes

Cadastro das pessoas que compram na loja.

- **Dono:** time de Cadastro e Atendimento
- **Classe:** pessoal (a do campo mais sensível)
- **Frescor prometido:** até 24 horas depois da mudança na origem
- **Retenção:** enquanto o cliente tiver conta ativa. Uso: o cadastro só serve enquanto há relação com o cliente. Encerrada a conta, ou quando o titular pede a eliminação (LGPD, art. 18), os dados pessoais saem de todas as camadas.
- **Acesso:** pedem times de Atendimento e de Análise; aprova time de Cadastro e Atendimento (dono); auditoria: toda leitura da bronze fica registrada; o analista só lê a silver
- **Contrato:** `contratos/clientes.yaml`, versão 3
- **Tabela bronze:** `workspace.lab_bronze.clientes` · 6 linhas · última alteração 2026-10-13 02:58
- **Tabela silver:** `workspace.lab_silver.clientes` · 6 linhas · última alteração 2026-10-15 23:46

| Campo | Tipo | Classe | Na silver | Regras | Descrição |
|---|---|---|---|---|---|
| `id` | inteiro | pessoal | pseudônimo `cliente_ps` | obrigatório, único | Identificador do cliente. Aponta para uma pessoa. |
| `nome` | texto | pessoal | removido | obrigatório | Nome completo. |
| `email` | texto | pessoal | removido | obrigatório | E-mail de contato. |
| `cpf` | texto | pessoal | removido | obrigatório | CPF do cliente. |
| `nascimento` | data | pessoal | mantido | obrigatório, máximo: momento da carga | Data de nascimento. Junto com a cidade, pode identificar a pessoa. |
| `cidade` | texto | pessoal | mantido | obrigatório | Cidade do cliente. |
| `atualizado_em` | data_hora | interno | mantido | obrigatório, máximo: momento da carga | Última alteração do cadastro na origem. |

## navegacao

Cliques no site da loja, um evento por página visitada.

- **Dono:** time do Site
- **Classe:** pessoal (a do campo mais sensível)
- **Frescor prometido:** até 5 segundos depois da publicação no tópico
- **Retenção:** 90 dias. Uso curto e volume alto: os cliques servem para análise de visitas recentes. Depois de 90 dias só os agregados ficam (visitas por página), sem IP e sem cliente.
- **Acesso:** pedem times de Marketing e de Produto; aprova time do Site (dono); auditoria: o IP nunca sai da bronze; leituras da bronze ficam registradas
- **Contrato:** `contratos/navegacao.yaml`, versão 3
- **Tabela bronze:** `workspace.lab_bronze.navegacao` · 8 linhas · última alteração 2026-10-15 00:07
- **Tabela silver:** `workspace.lab_silver.navegacao` · 8 linhas · última alteração 2026-10-15 23:46

| Campo | Tipo | Classe | Na silver | Regras | Descrição |
|---|---|---|---|---|---|
| `evento_id` | inteiro | interno | mantido | obrigatório, único | Identificador do evento. |
| `cliente_id` | inteiro | pessoal | pseudônimo `cliente_ps` | — | Cliente logado, quando houver. Vazio para visitante sem cadastro. |
| `ip` | texto | pessoal ⚠ achado pelo varredor | removido | obrigatório | Endereço IP da conexão. Identifica a conexão de uma pessoa. |
| `pagina` | texto | interno | mantido | obrigatório | Página visitada. |
| `quando` | data_hora | interno | mantido | obrigatório, máximo: momento da carga | Momento da visita. |

## pedidos

Pedidos feitos na loja, um por linha, com valor, status e o cliente que comprou.

- **Dono:** time de Vendas
- **Classe:** pessoal (a do campo mais sensível)
- **Frescor prometido:** até 24 horas depois da mudança na origem
- **Retenção:** 5 anos após o fim do ano do pedido. Obrigação fiscal: o pedido é registro de venda e precisa ser guardado pelo prazo de guarda fiscal (prazo a confirmar com o jurídico da loja). Se o cliente pedir a eliminação, o pedido fica, mas sem a pessoa: o vínculo com o cliente e a observação são apagados.
- **Acesso:** pedem times de Financeiro e de Análise; aprova time de Vendas (dono); auditoria: toda leitura da bronze fica registrada; o analista só lê a silver e a gold
- **Contrato:** `contratos/pedidos.yaml`, versão 3
- **Tabela bronze:** `workspace.lab_bronze.pedidos` · 9 linhas · última alteração 2026-10-13 02:58
- **Tabela silver:** `workspace.lab_silver.pedidos` · 11 linhas · última alteração 2026-10-15 23:46

| Campo | Tipo | Classe | Na silver | Regras | Descrição |
|---|---|---|---|---|---|
| `id` | inteiro | interno | mantido | obrigatório, único | Número do pedido. |
| `cliente_id` | inteiro | pessoal | pseudônimo `cliente_ps` | obrigatório | Cliente que fez o pedido. Liga o pedido a uma pessoa. |
| `valor_centavos` | inteiro | confidencial | mantido | obrigatório, mínimo 0 | Valor do pedido em centavos. |
| `status` | texto | interno | mantido | obrigatório, um de: pendente, pago, cancelado | Situação do pedido. |
| `criado_em` | data_hora | interno | mantido | obrigatório, máximo: momento da carga | Momento em que o pedido foi feito. |
| `observacao` | texto | pessoal ⚠ achado pelo varredor | removido | — | Texto livre do atendimento. Pode conter contato do cliente. |

---

Classes, da menos para a mais sensível: público < interno < confidencial < pessoal < pessoal sensível.
Regra como código (`governanca/05a_governanca`): conjunto de classe pessoal sem prazo de retenção para o pipeline.
Na silver: pseudônimo = HMAC-SHA-256 do id com a chave CHAVE_PSEUDONIMO (fora do repositório); proteção exigida por classe em `seguranca/matriz.md`.
