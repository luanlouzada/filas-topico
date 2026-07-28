// Package messaging concentra a integração com brokers compatíveis com a API Kafka.
package messaging

import (
	"os"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Tópicos usados pelo fluxo principal, pela DLQ e pelos testes de integração.
const (
	TicketEventsTopic            = "support.ticket-events.v1"
	TicketEventsDLQTopic         = "support.ticket-events.dlq.v1"
	TicketEventsIntegrationTopic = "support.ticket-events.integration.v1"
)

// Grupos diferentes mantêm offsets independentes e processam sua própria cópia
// lógica dos registros publicados no tópico principal.
const (
	SLAConsumerGroup          = "sla-service"
	NotificationConsumerGroup = "notification-service"
)

// Brokers lê KAFKA_BROKERS como uma lista separada por vírgulas. Quando a
// variável não está definida, usa o endereço exposto pelo Redpanda local.
func Brokers() []string {
	raw := strings.TrimSpace(os.Getenv("KAFKA_BROKERS"))
	if raw == "" {
		return []string{"localhost:19092"}
	}

	parts := strings.Split(raw, ",")
	brokers := make([]string, 0, len(parts))
	for _, part := range parts {
		if broker := strings.TrimSpace(part); broker != "" {
			brokers = append(brokers, broker)
		}
	}
	return brokers
}

// NewProducer configura um cliente Franz-go para publicar registros. NewClient
// apenas monta o cliente; a comunicação com o cluster começa quando ele precisa
// executar uma operação, como ProduceSync.
func NewProducer(brokers []string) (*kgo.Client, error) {
	return kgo.NewClient(
		// SeedBrokers informa endereços iniciais para o cliente entrar no cluster
		// e descobrir os demais brokers, tópicos, partições e seus líderes.
		kgo.SeedBrokers(brokers...),
		// RecordPartitioner escolhe a estratégia que transforma a Key do registro
		// em uma partição. StickyKey usa murmur2 quando o hasher é nil; assim, a
		// mesma chave ticketID permanece na mesma partição.
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
		// ClientID identifica este programa nas requisições, logs e métricas do
		// broker. Ele não autentica o cliente e não identifica a mensagem.
		kgo.ClientID("ticket-publisher"),
	)
}

// NewConsumer configura um cliente Franz-go para consumir como membro de um
// Consumer Group. As opções abaixo determinam de onde ele lê e quando confirma
// que terminou o processamento.
func NewConsumer(brokers []string, group string) (*kgo.Client, error) {
	return kgo.NewClient(
		// Usa os mesmos endereços iniciais do Producer para descobrir o cluster.
		kgo.SeedBrokers(brokers...),
		// ConsumerGroup faz esta instância entrar no grupo informado. O broker
		// distribui as partições entre as instâncias que usam o mesmo Group ID.
		kgo.ConsumerGroup(group),
		// ConsumeTopics declara o tópico que será consultado por PollRecords.
		kgo.ConsumeTopics(TicketEventsTopic),
		// Se o grupo ainda não possui offset confirmado para uma partição, começa
		// no primeiro registro disponível. Um offset existente tem precedência.
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		// Desativa a confirmação periódica do Franz-go. O código chama
		// CommitRecords somente depois do Handler ou da publicação na DLQ.
		kgo.DisableAutoCommit(),
		// Impede o rebalance durante o processamento dos registros retornados pelo
		// poll. Run chama AllowRebalance quando termina a mensagem atual.
		kgo.BlockRebalanceOnPoll(),
		// Um kgo.Client consumidor também pode produzir. Esta estratégia é usada
		// quando o mesmo cliente republica uma falha na DLQ, preservando sua chave.
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
		// Identifica a instância pelo nome lógico do grupo nas requisições e
		// métricas; não substitui o Group ID configurado em ConsumerGroup.
		kgo.ClientID(group),
	)
}
