# Mensageria mínima: Redpanda e franz-go

Este projeto apresenta tópicos, partições, chave de particionamento, publisher, consumer groups, retry e DLQ em um fluxo independente de Atendimento.

O projeto é um módulo Go independente. Todos os comandos Go e do Makefile devem ser executados a partir desta pasta.

O Redpanda implementa a API do Kafka. O cliente Go usado é [`franz-go`](https://github.com/twmb/franz-go), validado pelo próprio Redpanda. O mesmo código pode se conectar a Apache Kafka, Redpanda, Confluent Platform ou Amazon MSK; em outro ambiente mudam principalmente brokers, TLS, autenticação e configurações operacionais.

## Fluxo implementado

```text
publisher
   │ publica TicketEventV1
   │ key = ticketID
   ▼
support.ticket-events.v1
   ├── partition 0
   ├── partition 1
   └── partition 2
          │
          ├── group sla-service
          │      └── calcula a meta de restabelecimento
          │
          └── group notification-service
                 ├── prepara uma notificação
                 └── após três falhas → support.ticket-events.dlq.v1
```

Os dois grupos recebem os mesmos registros porque mantêm offsets independentes. Duas instâncias dentro do mesmo grupo dividem as partições; elas não recebem duas cópias do mesmo registro.

## Estrutura

```text
mensageria-minimal/
├── docker-compose.yml                 Redpanda, tópicos e Console
├── cmd/publisher/                     publica uma sequência fixa de quatro eventos
├── cmd/sla-consumer/                  consumer group sla-service
├── cmd/notification-consumer/         consumer group notification-service
├── internal/events/                   TicketEventV1 e DeadLetterV1
├── internal/handlers/                 reações que não conhecem Kafka
├── internal/messaging/                producer, consumer, retry, commit e DLQ
└── integration/redpanda_test.go       teste com broker real
```

## Conceitos observados no código

### Tópico

`support.ticket-events.v1` é o log no qual os fatos de Chamados são publicados. Ele foi criado com três partições.

### Chave de particionamento

O publisher usa `ticketID` como `Record.Key` e configura `StickyKeyPartitioner(nil)`. Nesse particionador, `nil` seleciona o hash `murmur2` compatível com o Kafka:

```text
TicketOpenedV1(ticket-001)   ─┐
                              ├── key ticket-001 ──> partition 2
TicketResolvedV1(ticket-001) ─┘
```

Eventos do mesmo chamado chegam à mesma partição e preservam sua ordem. Kafka não oferece uma ordem global entre todas as partições.

### Consumer group

```text
grupos diferentes                    mesmo grupo

registro ──> sla-service             partition 0 ──> worker A
        └──> notification-service     partition 1 ──> worker B
                                         partition 2 ──> worker A ou B
```

Grupos diferentes representam interesses diferentes. Instâncias do mesmo grupo representam trabalhadores concorrentes do mesmo interesse.

### Offset e commit

O offset é a posição do registro dentro da partição. O consumidor usa commit manual:

```text
ler registro
    ↓
executar Handler
    ↓
sucesso ou publicação na DLQ
    ↓
commit do offset
```

Se o processo falhar antes do commit, o registro poderá ser entregue novamente. Por isso consumidores reais precisam ser idempotentes.

### Retry e DLQ

Notificação tenta processar a mensagem até três vezes. Se a falha continuar, publica `DeadLetterV1` em `support.ticket-events.dlq.v1` e só então confirma o registro original.

```text
tentativa 1 ── falhou
tentativa 2 ── falhou
tentativa 3 ── falhou
                  ↓
                 DLQ
                  ↓
           commit do original
```

A DLQ preserva tópico, partição, offset, chave, erro e payload original. Kafka não possui uma DLQ automática universal: este é um padrão implementado pelo consumidor.

## Executar o fluxo

Todos os comandos partem desta pasta:

```bash
cd /home/luan_louzada/personal-projects/mensageria-minimal
```

### 1. Subir o broker e o Console

```bash
make infra-up
make topics
```

O Console fica em [http://localhost:8080](http://localhost:8080). Nele é possível observar tópicos, partições, chaves, payloads e consumer groups.

### 2. Iniciar SLA

No segundo terminal:

```bash
make run-sla
```

### 3. Iniciar Notificação com uma falha controlada

No terceiro terminal:

```bash
make run-notification-failing
```

A variável `FAIL_TICKET_ID` aponta para o terceiro chamado da sequência. A mensagem continua válida; somente o Handler retorna a indisponibilidade do provedor.

### 4. Publicar

No quarto terminal:

```bash
make publish
```

O publisher imprime tópico, partição e offset atribuídos. Os eventos de abertura e resolução do primeiro chamado devem aparecer na mesma partição.

### 5. Observar a DLQ

No Console, abra `support.ticket-events.dlq.v1`. O envelope mostra:

```json
{
  "sourceTopic": "support.ticket-events.v1",
  "sourcePartition": 2,
  "sourceOffset": 1,
  "key": "019535d9-3df7-7001-8000-000000000003",
  "attempts": 3,
  "error": "provedor de notificação indisponível",
  "failedAt": "...",
  "payload": {}
}
```

### Executar consumidores concorrentes

Pare a infraestrutura, remova os offsets anteriores e suba novamente:

```bash
make reset
make infra-up
```

Abra dois terminais com o mesmo comando:

```bash
make run-notification
```

Depois execute `make publish`. As duas instâncias usam o grupo `notification-service`, portanto as três partições são distribuídas entre elas.

## Testes

Testes locais, sem broker:

```bash
make test
```

Eles verificam contrato, serialização, chave usada pelo publisher, retry e handlers.

Teste de integração, depois de `make infra-up`:

```bash
make test-integration
```

O teste publica em um Redpanda real e prova que:

1. dois eventos com o mesmo `ticketID` recebem a mesma partição;
2. dois consumer groups diferentes leem o mesmo registro.

## Encerrar

Preservar os dados:

```bash
make infra-down
```

Remover containers, volume, mensagens e offsets:

```bash
make reset
```

## Kafka não é uma fila RabbitMQ

O consumer group produz um comportamento parecido com trabalhadores concorrentes, mas o modelo é diferente:

| Kafka / Redpanda | RabbitMQ |
|---|---|
| topic e partition | exchange e queue |
| key | routing key com outro papel |
| offset e commit | delivery tag e Ack/Nack |
| consumer group | competing consumers da fila |
| DLQ topic criada pela aplicação | dead-letter exchange/queue configurada no broker |

Ao trocar por RabbitMQ, o `Handler` e o contrato podem continuar úteis. O adapter de mensageria precisa ser refeito com `amqp091-go`, pois leitura, confirmação e roteamento possuem outra semântica.

## Referências

- [Redpanda: single broker com Console](https://docs.redpanda.com/labs/docker-compose/single-broker/)
- [Redpanda: clientes Kafka compatíveis](https://docs.redpanda.com/streaming/current/develop/kafka-clients/)
- [franz-go](https://github.com/twmb/franz-go)
