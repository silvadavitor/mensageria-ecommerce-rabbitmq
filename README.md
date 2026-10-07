# Mensageria de E-commerce com RabbitMQ

Trabalho prático de Sistemas Distribuídos (FURB).
Equipe: Pedro Bosini Freitag, Samuel Candido e Vitor da Silva.

A API recebe o pedido e publica um evento no RabbitMQ. Os serviços processam cada etapa de forma assíncrona e se comunicam só por mensagens.

```
Cliente ─▶ API de Pedidos ─(pedido.criado)─▶ Estoque ─(estoque.reservado)─▶ Pagamento ─(pagamento.aprovado)─▶ Logística

falha técnica            ─▶ retry (espera 5 s e tenta de novo, até 3 vezes)
erro fatal / 3ª falha    ─▶ DLX ─▶ DLQ
```

| Pasta | Serviço | Linguagem |
|-------|---------|-----------|
| `api-pedidos/` | API de Pedidos | C# (.NET 10) |
| `servico-estoque/` | Serviço de Estoque | Python |
| `servico-pagamento/` | Serviço de Pagamento | Python |
| `servico-logistica/` | Serviço de Logística | Go |
| `rabbitmq/` | exchanges, filas, DLX/DLQ e permissões | – |

## Passo a passo

### 1. Pré-requisito

Instale o [Docker](https://docs.docker.com/get-docker/) (ele já vem com o Docker Compose). Para conferir:

```bash
docker compose version
```

### 2. Baixar o projeto

```bash
git clone <url-do-repositorio>
cd mensageria-ecommerce-rabbitmq
```

### 3. Subir tudo

```bash
docker compose up -d --build
```

A primeira vez demora alguns minutos, porque as imagens são baixadas e compiladas. Para conferir se subiu:

```bash
docker compose ps
```

O `rabbitmq` deve aparecer como `healthy` e os outros serviços como `Up`. O `rabbitmq-init` aparece como `Exited (0)`, e está certo assim: ele só roda uma vez, para configurar o broker.

### 4. Criar um pedido

```bash
curl -X POST localhost:8080/pedidos -H "Content-Type: application/json" -d '{"clienteId":"cliente-1","itens":[{"produtoId":"SKU-1","quantidade":2,"precoUnitario":49.90}]}'
```

A resposta chega na hora (`202 Accepted`), e o pedido segue sendo processado em segundo plano. Para ver o pedido passando pelos serviços:

```bash
docker compose logs -f servico-estoque servico-pagamento servico-logistica
```

Aperte `Ctrl+C` para sair dos logs.

Se preferir, o arquivo [api-pedidos/pedidos.http](api-pedidos/pedidos.http) tem os mesmos exemplos, prontos para rodar no VS Code (extensão REST Client).

### 5. Ver o RabbitMQ funcionando

1. Abra http://localhost:15672.
2. Entre com usuário `admin` e senha `admin123`.
3. Mande vários pedidos de uma vez:

   ```bash
   for i in $(seq 1 50); do curl -s -o /dev/null -X POST localhost:8080/pedidos -H "Content-Type: application/json" -d '{"clienteId":"c'$i'","itens":[{"produtoId":"SKU-1","quantidade":1,"precoUnitario":10}]}'; done
   ```

4. No painel, abra a aba **Queues**. Com os erros aleatórios ligados, algumas mensagens vão aparecer na `ecommerce.dlq`.
5. Clique em `ecommerce.dlq` → **Get messages** para ver as mensagens. O header `x-death` mostra de qual fila cada uma veio e por quê.

### 6. Escalar um serviço

```bash
docker compose up -d --scale servico-estoque=3
```

Na aba **Queues**, a fila `estoque.reservar` passa a ter 3 consumidores dividindo as mensagens. Para deixar isso fixo, altere `ESTOQUE_REPLICAS` (ou `PAGAMENTO_REPLICAS`, `LOGISTICA_REPLICAS`) no `.env`.

### 7. Desligar

```bash
docker compose down        # para tudo
docker compose down -v     # para tudo e apaga filas e mensagens
```

## Configurações do `.env`

O `.env` já vem pronto no repositório, com senhas simples, porque o projeto roda só localmente. Não precisa mexer em nada para funcionar.
Em um ambiente real, o `.env` não deve ser versionado no repositório. Neste projeto, ele está presente apenas para facilitar os testes e a execução local. Em um cenário de produção, o arquivo seria adicionado ao .`.gitignore` e as credenciais seriam configuradas de forma segura.

| Variável | Padrão | O que faz |
|----------|--------|-----------|
| `FALHA_PERCENTUAL` | 10 | % de falhas técnicas simuladas, que vão para o retry |
| `FALHA_FATAL_PERCENTUAL` | 5 | % de erros fatais simulados, que vão direto para a DLQ |
| `MAX_TENTATIVAS` | 3 | quantas tentativas antes de mandar para a DLQ |
| `PREFETCH` | 10 | quantas mensagens cada container pega por vez |
| `ESTOQUE_REPLICAS`, `PAGAMENTO_REPLICAS`, `LOGISTICA_REPLICAS` | 1 | quantos containers de cada serviço |
| `API_PORT`, `RABBITMQ_MANAGEMENT_PORT` | 8080, 15672 | portas no seu computador |
| `*_USER` / `*_PASS` | – | um usuário do RabbitMQ para cada serviço |

Depois de alterar o `.env`, rode `docker compose up -d` de novo para aplicar.

## RabbitMQ

| Exchange | Tipo | Para quê |
|----------|------|----------|
| `ecommerce.eventos` | topic | exchange central, roteia pelo nome do evento |
| `ecommerce.retry` | fanout | recebe as mensagens que vão ser tentadas de novo |
| `ecommerce.dlx` | fanout | Dead Letter Exchange |

| Fila | Recebe | Consumidor |
|------|--------|------------|
| `estoque.reservar` | `pedido.criado` | Estoque |
| `pagamento.processar` | `estoque.reservado` | Pagamento |
| `logistica.agendar` | `pagamento.aprovado` | Logística |
| `ecommerce.retry.espera` | mensagens em retry, que ficam 5 s e voltam | – |
| `ecommerce.dlq` | mensagens com erro | inspeção manual |

- As filas principais têm TTL de 10 min. Uma mensagem parada por mais tempo que isso vai para a DLQ.
- Filas duráveis e mensagens persistentes: nada se perde se o broker reiniciar.
- Ack manual: se um container cair no meio do processamento, a mensagem volta para a fila.
- Cada serviço tem seu próprio usuário e só consegue ler a própria fila.

Toda a topologia está em [rabbitmq/topologia.json](rabbitmq/topologia.json).

## Problemas comuns

| Problema | Solução |
|----------|---------|
| `rabbitmq-init didn't complete successfully` | alguma linha do `.env` ficou vazia: `docker compose logs rabbitmq-init` mostra qual |
| `port is already allocated` | a porta 8080 ou 15672 já está em uso: troque `API_PORT` ou `RABBITMQ_MANAGEMENT_PORT` no `.env` |
| trocou uma senha no `.env` e ela não funciona | rode `docker compose up -d --force-recreate` |
| API responde `503` | o RabbitMQ ainda está subindo: espere alguns segundos |
