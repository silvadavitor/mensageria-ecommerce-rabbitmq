# Configuração, Exemplos de Uso e Considerações Técnicas

Complementa o [Mensageria.pdf](Mensageria.pdf) e o [README](../README.md). 

## 3. Configuração do RabbitMQ

### 3.1 Como a configuração é carregada

A topologia (exchanges, filas, bindings e permissões) fica em [rabbitmq/topologia.json](../rabbitmq/topologia.json). Ao subir o ambiente, o container `rabbitmq-init` executa [rabbitmq/gerar_definitions.py](../rabbitmq/gerar_definitions.py), que junta essa topologia com os usuários do `.env` e gera um `definitions.json`. O broker carrega esse arquivo a cada inicialização (`load_definitions` em [rabbitmq/rabbitmq.conf](../rabbitmq/rabbitmq.conf)).

Os serviços não declaram filas nem exchanges: eles só publicam e consomem. Assim a topologia fica num único lugar, versionada junto com o código. Todos os recursos ficam no virtual host `ecommerce`.

### 3.2 Argumentos das filas e bindings

| Fila | Argumentos | Binding |
|---|---|---|
| `estoque.reservar` | TTL 600000 ms, DLX `ecommerce.dlx` | `ecommerce.eventos` → `pedido.criado` |
| `pagamento.processar` | TTL 600000 ms, DLX `ecommerce.dlx` | `ecommerce.eventos` → `estoque.reservado` |
| `logistica.agendar` | TTL 600000 ms, DLX `ecommerce.dlx` | `ecommerce.eventos` → `pagamento.aprovado` |
| `ecommerce.retry.espera` | TTL 5000 ms, DLX `ecommerce.eventos` | `ecommerce.retry` (fanout) |
| `ecommerce.dlq` | – | `ecommerce.dlx` (fanout) |

Todas as filas são `classic`, duráveis e sem `auto_delete`.

### 3.3 Política de retry

O documento (seção 5.2) prevê até três tentativas antes da DLQ, mas não define como elas são feitas. Para implementar as três tentativas, foi necessário criar dois componentes que não aparecem na Figura 1 do documento:

- a exchange `ecommerce.retry` (fanout), que recebe as mensagens que vão ser tentadas de novo;
- a fila `ecommerce.retry.espera`, onde a mensagem espera 5 s antes de voltar para a fila de origem.

O fluxo é este:

1. O consumidor tem uma falha técnica.
2. Ele lê o header `x-tentativas` da mensagem (0 se não existir) e soma 1.
3. Se ainda não chegou em `MAX_TENTATIVAS` (padrão 3), republica o mesmo corpo na exchange `ecommerce.retry`, com a routing key original e o novo `x-tentativas`, e dá `ack` na mensagem original.
4. A mensagem fica 5 s em `ecommerce.retry.espera`. Quando o TTL expira, o RabbitMQ faz o dead-letter dela para `ecommerce.eventos`. Como a fila não define `x-dead-letter-routing-key`, a routing key original é mantida e a mensagem volta para a mesma fila de onde saiu.
5. Na última tentativa, o consumidor dá `nack(requeue=false)` e a mensagem vai para a DLQ.

Uma única fila de espera atende os três serviços, porque o caminho de volta é definido pela routing key que a mensagem já carrega.

O retry não usa `requeue=true` porque isso devolveria a mensagem na hora, e o consumidor ficaria repetindo a mesma falha sem nenhum intervalo.

### 3.4 Quando a falha técnica pula o retry

Toda falha técnica (seção 5.1 do documento) termina com `nack(requeue=false)` e vai para a DLQ. A diferença está em quando isso acontece: algumas falhas só se resolvem tentando de novo, e outras nunca se resolvem.

| Falha técnica | Exemplos | Tratamento |
|---|---|---|
| Pode se resolver sozinha | timeout, serviço externo indisponível, exceção inesperada | retry (3.3); `nack` só depois da 3ª tentativa |
| Não se resolve repetindo | mensagem malformada (JSON inválido, campo obrigatório ausente, `itens` vazio), erro simulado do tipo fatal | `nack` já na 1ª tentativa |

Repetir uma mensagem malformada daria o mesmo erro três vezes. Por isso ela vai direto para a DLQ.

**Falhas simuladas:** os serviços não têm falhas reais (o estoque, o pagamento e a transportadora são simulados), então, sem uma falha forçada, a DLX e a DLQ nunca receberiam mensagens. Por isso, como previsto na seção 5 do documento, os erros são sorteados ao acaso: `FALHA_PERCENTUAL` (padrão 10%) gera falhas técnicas que passam pelo retry, e `FALHA_FATAL_PERCENTUAL` (padrão 5%) gera falhas que vão direto para a DLQ. Assim é possível ver o retry, a DLX e a DLQ funcionando. Com os dois valores em `0` no `.env`, o sistema roda sem nenhuma falha.

### 3.5 Inspeção e reprocessamento da DLQ

O RabbitMQ adiciona o header `x-death` a cada mensagem que chega na DLQ, com a fila de origem, o motivo (`rejected` para nack, `expired` para TTL), o número de ocorrências e o horário. O plugin `rabbitmq_shovel_management` está habilitado, o que permite mover mensagens da DLQ de volta para `ecommerce.eventos` pelo painel, depois de corrigir a causa.

### 3.6 Parâmetros de conexão dos clientes

| Parâmetro | Valor | Efeito |
|---|---|---|
| Heartbeat | 30 s | detecta conexões mortas |
| Reconexão dos consumidores | a cada 3 s | se a conexão cai, o serviço tenta de novo até conseguir |
| Reconexão da API | na próxima requisição | se uma publicação falha, a conexão é descartada e reaberta no próximo pedido |
| Nome da conexão | `<serviço>@<container>` | identifica cada réplica no painel |

### 3.7 Requisitos de segurança

#### Autenticação

- Cada componente usa o próprio usuário (`api_pedidos`, `svc_estoque`, `svc_pagamento`, `svc_logistica`), e só o `admin` acessa o painel. Se uma credencial vazar, o estrago fica limitado àquele serviço, e é possível revogar um usuário sem afetar os outros.
- O `definitions.json` recebe apenas o hash das senhas (salt aleatório + SHA-256, formato `rabbit_password_hashing_sha256`). Nenhuma senha fica no `topologia.json`.
- Como os usuários são carregados pelo `definitions.json` na inicialização, o RabbitMQ não cria o usuário padrão `guest`.
- Se faltar alguma credencial, nada sobe: o `rabbitmq-init` falha, a API valida a configuração na partida (`ValidateOnStart`) e os serviços Python e Go encerram com erro.

#### Autorização (menor privilégio)

| Usuário | configure | write (publicar em) | read (consumir de) |
|---|---|---|---|
| `admin` | tudo | tudo | tudo |
| `api_pedidos` | nada | `ecommerce.eventos` | nada |
| `svc_estoque` | nada | `ecommerce.eventos`, `ecommerce.retry` | `estoque.reservar` |
| `svc_pagamento` | nada | `ecommerce.eventos`, `ecommerce.retry` | `pagamento.processar` |
| `svc_logistica` | nada | `ecommerce.retry` | `logistica.agendar` |

- Nenhum serviço tem permissão `configure`, então nenhum consegue criar, alterar ou apagar exchanges e filas.
- Cada serviço só lê a própria fila.
- A Logística é o fim do fluxo, então só pode publicar retries, não eventos de negócio.
- O vhost `ecommerce` isola os recursos do projeto de outros vhosts no mesmo broker.

#### Exposição de rede

A porta AMQP (5672) não é publicada para o computador host: só é acessível na rede interna do Docker Compose. Só a API (8080) e o painel (15672) ficam expostos.

#### Criptografia

A criptografia não foi configurada, porque o projeto é para uso local, em ambiente de desenvolvimento (ver a seção *Criptografia* do [README](../README.md#criptografia)). Para levar o projeto para produção, seriam configurados:

- **TLS na comunicação:** só AMQPS (porta 5671), com a porta 5672 desligada, e o painel em HTTPS. Os clientes se conectariam por `amqps://` validando o certificado do broker (as três bibliotecas usadas suportam TLS).
- **Criptografia do volume do RabbitMQ:** o disco do `rabbitmq-data` seria criptografado (por exemplo, LUKS ou um volume criptografado do provedor de nuvem), protegendo as mensagens persistidas.
- **Segredos fora do repositório:** as senhas sairiam do `.env` versionado e iriam para um gerenciador de segredos (Docker secrets, Vault etc.).

---

## 4. Exemplos de Uso

Configuração padrão do `.env`. As requisições também estão em [api-pedidos/pedidos.http](../api-pedidos/pedidos.http).

### 4.1 Pedido processado com sucesso

**Entrada**

```http
POST /pedidos
Content-Type: application/json

{
  "clienteId": "cliente-42",
  "itens": [
    { "produtoId": "SKU-001", "quantidade": 2, "precoUnitario": 49.90 },
    { "produtoId": "SKU-777", "quantidade": 1, "precoUnitario": 199.00 }
  ]
}
```

**Processamento:** a API valida o corpo, gera o `pedidoId`, calcula `valorTotal` = 2 × 49,90 + 199,00 = 298,80, salva o pedido e publica `pedido.criado`. Ela só responde depois que o broker confirma que gravou a mensagem. Em seguida, Estoque, Pagamento e Logística processam em sequência, como no fluxo principal do documento.

**Saída**

```http
HTTP/1.1 202 Accepted
Location: http://localhost:8080/pedidos/3f2c…

{ "pedidoId": "3f2c…", "status": "Recebido", "valorTotal": 298.80 }
```

```
[servico-estoque]   pedido 3f2c…: itens reservados (SKU-001 x2, SKU-777 x1)
[servico-estoque]   pedido 3f2c…: publicado 'estoque.reservado'
[servico-pagamento] pedido 3f2c…: cobrança de R$ 298.80 aprovada (transação a91c04e7b2d1)
[servico-pagamento] pedido 3f2c…: publicado 'pagamento.aprovado'
servico=servico-logistica msg="entrega agendada" pedidoId=3f2c… rastreio=BR048213377 previsao=10/10/2026
```

### 4.2 Pedido inválido

**Entrada:** `POST /pedidos` com `{ "clienteId": "cliente-42", "itens": [] }`.

**Processamento:** a validação da API rejeita a requisição. As regras são: pelo menos 1 e no máximo 100 itens, `quantidade` de 1 a 10.000 e `precoUnitario` de 0,01 a 1.000.000. Nada é publicado.

**Saída:** `400 Bad Request` com `ValidationProblemDetails` indicando o campo com erro.

### 4.3 Broker indisponível

**Entrada:** pedido válido com o RabbitMQ parado (`docker compose stop rabbitmq`).

**Processamento:** a publicação falha. A API marca o pedido como `FalhaAoPublicar`, registra o erro no log e descarta a conexão.

**Saída:** `503 Service Unavailable` (`"title": "Broker de mensagens indisponível"`). A API nunca responde `202` para um pedido que não chegou ao broker.

### 4.4 Falha técnica temporária (retry com sucesso)

**Entrada:** pedido cuja primeira tentativa no Pagamento falha (por exemplo, um timeout do gateway, simulado por `FALHA_PERCENTUAL`).

**Processamento:** o Pagamento republica a mensagem em `ecommerce.retry` com `x-tentativas: 1` e dá `ack` na original. Depois de 5 s, a mensagem volta para `pagamento.processar`, e a tentativa 2 funciona.

**Saída**

```
WARNING [servico-pagamento] pedido 3f2c…: falha na tentativa 1/3 (falha técnica simulada), vai tentar de novo
INFO    [servico-pagamento] pedido 3f2c…: cobrança de R$ 298.80 aprovada (transação …)
INFO    [servico-pagamento] pedido 3f2c…: publicado 'pagamento.aprovado'
```

### 4.5 Tentativas esgotadas

**Entrada:** pedido que falha 3 vezes seguidas no Estoque.

**Processamento:** as tentativas 1 e 2 passam pelo retry. Na 3ª, o Estoque dá `nack(requeue=false)` e a mensagem vai para a DLX.

**Saída:** log `pedido 3f2c…: falhou 3 vezes -> DLQ`. A mensagem fica em `ecommerce.dlq` com `x-tentativas: 2` e `x-death` (`queue: estoque.reservar`, `reason: rejected`). Nenhum `estoque.reservado` é publicado, então o cliente não é cobrado.

### 4.6 Mensagem malformada ou erro fatal

**Entrada:** mensagem sem `pedidoId` publicada manualmente pelo painel em `ecommerce.eventos` com routing key `pedido.criado`.

**Processamento:** a validação do consumidor falha e a mensagem vai direto para a DLQ, sem passar pelo retry.

**Saída:** log `mensagem inválida -> DLQ: mensagem sem os campos obrigatórios`, e a mensagem aparece em `ecommerce.dlq`.

### 4.7 Mensagem duplicada

**Entrada:** o Estoque publica `estoque.reservado`, mas o container cai antes do `ack`. O RabbitMQ entrega `pedido.criado` de novo, e o `estoque.reservado` é publicado uma segunda vez.

**Processamento:** o `messageId` gerado pelos serviços é um UUID v5 de `pedidoId/evento`, então as duas publicações têm o mesmo `messageId`. O Pagamento reconhece o `messageId` como já processado e apenas dá `ack`.

**Saída:** log `pedido 3f2c…: mensagem repetida, ignorando`. Não há cobrança dupla.

### 4.8 Queda de uma réplica sob carga

**Entrada:** `docker compose up -d --scale servico-estoque=3`, uma rajada de 50 pedidos e `docker stop` em uma das réplicas durante o processamento.

**Processamento:** as mensagens que a réplica parada tinha recebido sem dar `ack` voltam para `estoque.reservar` e são processadas pelas outras duas.

**Saída:** todos os pedidos chegam à Logística, exceto os desviados para a DLQ pelas falhas simuladas.

### 4.9 Serviço fora do ar por mais de 10 minutos

**Entrada:** `docker compose stop servico-logistica`, seguido de novos pedidos.

**Processamento:** as mensagens se acumulam em `logistica.agendar`. As que passam de 10 min sem ser consumidas expiram.

**Saída:** as mensagens expiradas aparecem em `ecommerce.dlq` com `x-death.reason: expired`.

---

## 5. Considerações Técnicas

### 5.1 Tecnologias

| Componente | Linguagem / runtime | Biblioteca AMQP |
|---|---|---|
| API de Pedidos | C# / .NET 10 (ASP.NET Core) | `RabbitMQ.Client` 7.2.2 |
| Estoque e Pagamento | Python 3.13 | `pika` 1.3.2 |
| Logística | Go 1.22 | `rabbitmq/amqp091-go` 1.10.0 |
| Broker | RabbitMQ 4.1 (plugins management e shovel) | – |
| Infraestrutura | Docker e Docker Compose | – |

Usar três linguagens mostra que o protocolo AMQP 0-9-1 desacopla os serviços também em tecnologia: cada um só precisa conhecer o formato da mensagem e o nome do evento.

### 5.2 Padrão de mensagens

**Por que JSON:** é legível no painel (o que facilita analisar a DLQ), tem suporte nativo nas três linguagens e não exige compilar schemas. Avro ou Protobuf gerariam mensagens menores e com schema validado, mas exigiriam registro de schemas e geração de código, um custo que não se justifica no volume deste projeto.

**Tipos dos campos** (a lista de campos está na seção 4.2 do documento):

| Campo | Tipo |
|---|---|
| `pedidoId` | string (UUID); é o mesmo em todos os eventos e serve como ID de correlação |
| `clienteId` | string |
| `itens` | lista não vazia de `{ produtoId: string, quantidade: inteiro, precoUnitario: número }` |
| `valorTotal` | número |
| `messageId` | string (UUID): aleatório na API, UUID v5 de `pedidoId/evento` nos serviços |
| `timestamp` | string ISO 8601 em UTC |

**Propriedades e headers AMQP**

| Propriedade / header | Valor |
|---|---|
| `content_type` | `application/json` |
| `message_id` | igual ao `messageId` do corpo |
| `app_id` | quem publicou (`api-pedidos`, `servico-estoque`…) |
| `x-tentativas` | número de falhas anteriores (só em mensagens que passaram pelo retry) |
| `x-death` | adicionado pelo RabbitMQ no dead-letter |

**Routing keys:** seguem o padrão `<entidade>.<fato no passado>`. As mensagens são eventos (algo que já aconteceu), e não comandos enviados a um serviço específico.

### 5.3 Boas práticas adotadas

Além de durabilidade, ack manual e prefetch (seção 7 do documento):

**Integridade**

- **Publisher confirms:** API e serviços só consideram uma mensagem publicada depois que o broker confirma. Na API, isso garante que o `202` só é enviado com o evento já gravado no broker.
- **`mandatory = true`:** se nenhuma fila estiver ligada à routing key, a publicação dá erro em vez de a mensagem ser descartada sem aviso.
- **Ack só depois de publicar o próximo evento:** se o serviço cair entre as duas operações, a mensagem é entregue de novo. A garantia é *at-least-once*.
- **Idempotência por `messageId`:** cobre as reentregas causadas pelo *at-least-once* (exemplo 4.7). *Limitação:* o registro dos `messageId` processados fica na memória de cada réplica, então se perde quando o container reinicia e não é compartilhado entre réplicas. Em produção, ele deveria ficar num Redis ou numa tabela com chave única.
- **Validação do corpo no consumidor:** mensagens malformadas vão direto para a DLQ e não travam a fila.
- **Encerramento controlado:** os serviços tratam `SIGTERM` e fecham a conexão; as mensagens sem `ack` voltam para a fila.
- **Retry com intervalo e limite de tentativas,** em vez de `requeue=true` (seção 3.3).

**Desempenho**

- A API mantém uma única conexão e um único canal com o broker (singleton), em vez de abrir uma conexão por requisição.
- A API responde `202` sem esperar as etapas seguintes, então o tempo de resposta não depende de estoque, pagamento ou logística.
- Cada serviço tem a própria fila: um serviço lento só acumula mensagens na fila dele, sem atrasar os outros.

**Observabilidade**

- Conexões nomeadas por réplica e logs com o `pedidoId` em todas as etapas, o que permite acompanhar um pedido de ponta a ponta.
