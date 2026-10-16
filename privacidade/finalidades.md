# Finalidade dos campos pessoais da silver

Regra: **campo pessoal que fica na silver precisa de uma finalidade registrada no contrato** (`finalidade`).
Campo sem finalidade sai (`protecao: remover`). A regra como código (`governanca/05a_governanca`) para o pipeline
se encontrar campo pessoal na silver sem finalidade. O catálogo mostra a finalidade de cada campo.

| Conjunto | Campo | Na silver | Finalidade |
|---|---|---|---|
| clientes | `id` | pseudônimo `cliente_ps` | ligar pedidos e navegação ao mesmo cliente sem identificá-lo |
| clientes | `nascimento` | faixa etária (`faixa_etaria`) | público por faixa etária; a data exata não tem uso, então só a faixa entra |
| clientes | `cidade` | mantida | receita e público por cidade (visão gold `receita_por_cidade`, com limiar K) |
| clientes | `nome`, `email`, `cpf` | removidos | nenhuma análise precisa deles |
| pedidos | `cliente_id` | pseudônimo `cliente_ps` | contar clientes e ligar o pedido à cidade do cliente |
| pedidos | `observacao` | removida | texto livre, sem uso analítico, com contato do cliente |
| navegacao | `cliente_id` | pseudônimo `cliente_ps` | visitas por cliente logado |
| navegacao | `ip` | removido | nenhuma análise precisa da conexão |

## Pseudônimo não é anônimo

Na silver não há nome, e-mail nem CPF, mas há `cidade` e `faixa_etaria`. Juntas, elas são **quase-identificadores**:
quem sabe que a Ana mora em Santos e tem 36 anos acha a linha dela se for a única nessa combinação, e com o
`cliente_ps` chega a todos os pedidos e cliques dela.

**K-anonimato** mede isso: agrupando os clientes pelos quase-identificadores, K é o tamanho do menor grupo. Com K = 1
existe pelo menos uma pessoa sozinha na combinação. A medição está em `evidencias/missao7.txt` e em
`lab_controle.medicoes_k`.

O que fizemos:

- **Generalizar:** a data de nascimento virou faixa etária (25–34, 35–44, 45–54…). A data exata identificava a
  pessoa quase sozinha e não tinha finalidade.
- **Agregar e suprimir no consumo:** a visão `lab_gold.receita_por_cidade` não tem linha por pessoa, só por cidade, e
  cidade com menos de K clientes (K = 2) não aparece pelo nome.
- **Não deixar recalcular por diferença:** a receita das cidades suprimidas entra numa linha só, junto com os pedidos
  sem cliente (como o 106, da cliente que pediu a eliminação). A soma das linhas continua sendo a receita total
  (R$ 844,90), mas ninguém descobre a receita de Campinas subtraindo Santos e Sorocaba do total.
- **K maior protege mais e mostra menos:** com K = 3, Sorocaba (2 clientes) também é suprimida.
