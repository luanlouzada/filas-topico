package messaging

import (
	"context"
	"testing"
	"time"

	"mensageria-minimal/internal/events"

	"github.com/twmb/franz-go/pkg/kgo"
)

// producerSpy guarda o registro entregue ao client sem abrir uma conexão real.
type producerSpy struct {
	records []*kgo.Record
}

// ProduceSync imita a confirmação do broker com partição e offset conhecidos.
func (producer *producerSpy) ProduceSync(
	_ context.Context,
	records ...*kgo.Record,
) kgo.ProduceResults {
	producer.records = append(producer.records, records...)
	records[0].Partition = 2
	records[0].Offset = 7
	return kgo.ProduceResults{{Record: records[0]}}
}

// TestPublisherUsesTicketIDAsRecordKey verifica a decisão de particionamento
// sem tentar reproduzir o algoritmo interno do client Kafka.
func TestPublisherUsesTicketIDAsRecordKey(t *testing.T) {
	t.Parallel()

	spy := &producerSpy{}
	publisher := NewPublisher(spy, TicketEventsTopic)
	event := events.TicketEventV1{
		EventID: "event-1", Type: events.TicketOpenedV1,
		TicketID: "ticket-42", Subject: "Sem internet", Priority: "HIGH",
		OccurredAt: time.Date(2027, time.March, 15, 9, 30, 0, 0, time.UTC),
	}

	result, err := publisher.Publish(t.Context(), event)
	if err != nil {
		t.Fatalf("Publish() erro = %v", err)
	}
	if len(spy.records) != 1 {
		t.Fatalf("registros publicados = %d; esperado = 1", len(spy.records))
	}
	if got := string(spy.records[0].Key); got != event.TicketID {
		t.Fatalf("chave = %q; esperada = %q", got, event.TicketID)
	}
	if result.Partition != 2 || result.Offset != 7 {
		t.Fatalf("resultado = %#v; esperada partition=2 offset=7", result)
	}
}
