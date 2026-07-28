package messaging

import (
	"context"
	"errors"

	"mensageria-minimal/internal/events"

	"github.com/twmb/franz-go/pkg/kgo"
)

// syncProducer descreve somente a operação necessária ao Publisher.
// *kgo.Client satisfaz essa interface e os testes podem usar uma implementação pequena.
type syncProducer interface {
	ProduceSync(context.Context, ...*kgo.Record) kgo.ProduceResults
}

// PublishResult informa a posição atribuída pelo broker ao registro confirmado.
type PublishResult struct {
	Topic     string
	Partition int32
	Offset    int64
}

// Publisher publica contratos no tópico informado.
type Publisher struct {
	producer syncProducer
	topic    string
}

// NewPublisher liga uma implementação de produção ao tópico escolhido.
func NewPublisher(producer syncProducer, topic string) *Publisher {
	return &Publisher{producer: producer, topic: topic}
}

// Publish usa ticketID como chave. O particionador transforma essa chave em partição.
func (publisher *Publisher) Publish(
	ctx context.Context,
	event events.TicketEventV1,
) (PublishResult, error) {
	payload, err := event.Marshal()
	if err != nil {
		return PublishResult{}, err
	}

	// Record é a unidade que o Franz-go envia ao broker. Topic seleciona o log;
	// Key participa do particionamento; Value contém o contrato serializado; e
	// Headers transportam informações auxiliares sem alterar o payload.
	record := &kgo.Record{
		Topic: publisher.topic,
		Key:   []byte(event.TicketID),
		Value: payload,
		Headers: []kgo.RecordHeader{
			{Key: "event-type", Value: []byte(event.Type)},
			{Key: "content-type", Value: []byte("application/json")},
		},
	}
	// ProduceSync envia o registro e aguarda o resultado. O Franz-go devolve no
	// próprio Record a partição e o offset atribuídos pelo broker.
	results := publisher.producer.ProduceSync(ctx, record)
	// FirstErr procura o primeiro erro entre os resultados da publicação.
	if err := results.FirstErr(); err != nil {
		return PublishResult{}, err
	}
	if len(results) != 1 || results[0].Record == nil {
		return PublishResult{}, errors.New("broker não devolveu o resultado da publicação")
	}

	stored := results[0].Record
	return PublishResult{
		Topic: stored.Topic, Partition: stored.Partition, Offset: stored.Offset,
	}, nil
}
