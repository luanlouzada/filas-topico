//go:build integration

// Package integration verifica o contrato do código com um broker Redpanda real.
package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"mensageria-minimal/internal/events"
	"mensageria-minimal/internal/messaging"

	"github.com/twmb/franz-go/pkg/kgo"
)

// TestRedpandaPartitionKeyAndConsumerGroups prova duas propriedades importantes:
// a mesma chave escolhe a mesma partição e grupos distintos recebem o mesmo fato.
func TestRedpandaPartitionKeyAndConsumerGroups(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()

	producer, err := messaging.NewProducer(messaging.Brokers())
	if err != nil {
		t.Fatalf("criar producer: %v", err)
	}
	defer producer.Close()

	unique := fmt.Sprintf("%d", time.Now().UnixNano())
	openedAt := time.Date(2027, time.March, 15, 9, 30, 0, 0, time.UTC)
	opened := events.TicketEventV1{
		EventID:  "integration-opened-" + unique,
		Type:     events.TicketOpenedV1,
		TicketID: "integration-ticket-" + unique,
		Subject:  "Teste com Redpanda real", Priority: "HIGH", OccurredAt: openedAt,
	}
	resolved := events.TicketEventV1{
		EventID:    "integration-resolved-" + unique,
		Type:       events.TicketResolvedV1,
		TicketID:   opened.TicketID,
		Resolution: "Teste concluído", OccurredAt: openedAt.Add(time.Hour),
	}

	publisher := messaging.NewPublisher(
		producer, messaging.TicketEventsIntegrationTopic,
	)
	first, err := publisher.Publish(ctx, opened)
	if err != nil {
		t.Fatalf("publicar TicketOpenedV1: %v", err)
	}
	second, err := publisher.Publish(ctx, resolved)
	if err != nil {
		t.Fatalf("publicar TicketResolvedV1: %v", err)
	}
	if first.Partition != second.Partition {
		t.Fatalf(
			"mesma chave foi para partições diferentes: %d e %d",
			first.Partition, second.Partition,
		)
	}

	groupARecord := consumeEvent(
		t, ctx, "integration-sla-"+unique, opened.EventID,
	)
	groupBRecord := consumeEvent(
		t, ctx, "integration-notification-"+unique, opened.EventID,
	)
	if groupARecord.Offset != groupBRecord.Offset ||
		groupARecord.Partition != groupBRecord.Partition {
		t.Fatalf(
			"grupos não observaram o mesmo registro: A=%d/%d B=%d/%d",
			groupARecord.Partition, groupARecord.Offset,
			groupBRecord.Partition, groupBRecord.Offset,
		)
	}
}

// consumeEvent cria um grupo exclusivo e procura o EventID produzido pelo
// teste. Grupos exclusivos impedem que offsets de outra execução interfiram.
func consumeEvent(
	t *testing.T,
	ctx context.Context,
	group string,
	eventID string,
) *kgo.Record {
	t.Helper()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(messaging.Brokers()...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(messaging.TicketEventsIntegrationTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("criar consumer %s: %v", group, err)
	}
	defer client.Close()

	for {
		fetches := client.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("aguardar evento %s no grupo %s: %v", eventID, group, ctx.Err())
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			t.Fatalf("consultar broker no grupo %s: %v", group, errs)
		}
		for _, record := range fetches.Records() {
			event, parseErr := events.ParseTicketEvent(record.Value)
			if parseErr == nil && event.EventID == eventID {
				return record
			}
		}
	}
}
