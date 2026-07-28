// O executável publisher envia fatos de Chamados para o tópico de integração.
package main

import (
	"context"
	"log"
	"time"

	"mensageria-minimal/internal/events"
	"mensageria-minimal/internal/messaging"
)

// main monta o cliente Kafka, publica uma sequência conhecida de eventos e
// termina depois que o broker confirma tópico, partição e offset de cada registro.
func main() {
	// O timeout evita que o processo espere indefinidamente por um broker indisponível.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	client, err := messaging.NewProducer(messaging.Brokers())
	if err != nil {
		log.Fatalf("criar producer: %v", err)
	}
	defer client.Close()

	publisher := messaging.NewPublisher(client, messaging.TicketEventsTopic)
	for _, event := range eventsToPublish() {
		result, err := publisher.Publish(ctx, event)
		if err != nil {
			log.Fatalf("publicar %s: %v", event.EventID, err)
		}
		log.Printf(
			"publicado type=%s ticketID=%s topic=%s partition=%d offset=%d",
			event.Type, event.TicketID, result.Topic, result.Partition, result.Offset,
		)
	}
}

// eventsToPublish cria uma sequência reproduzível. A abertura e a resolução do
// primeiro chamado usam a mesma chave e, portanto, chegam à mesma partição.
func eventsToPublish() []events.TicketEventV1 {
	openedAt := time.Date(2027, time.March, 15, 9, 30, 0, 0, time.UTC)
	return []events.TicketEventV1{
		{
			EventID:  "019535d9-3df7-7001-8000-000000000101",
			Type:     events.TicketOpenedV1,
			TicketID: "019535d9-3df7-7001-8000-000000000001",
			Subject:  "Cliente sem acesso à internet", Priority: "HIGH",
			OccurredAt: openedAt,
		},
		{
			EventID:    "019535d9-3df7-7001-8000-000000000102",
			Type:       events.TicketResolvedV1,
			TicketID:   "019535d9-3df7-7001-8000-000000000001",
			Resolution: "Roteador reiniciado e conexão restabelecida",
			OccurredAt: openedAt.Add(90 * time.Minute),
		},
		{
			EventID:  "019535d9-3df7-7001-8000-000000000103",
			Type:     events.TicketOpenedV1,
			TicketID: "019535d9-3df7-7001-8000-000000000002",
			Subject:  "Sinal instável no escritório", Priority: "NORMAL",
			OccurredAt: openedAt.Add(2 * time.Minute),
		},
		{
			EventID:  "019535d9-3df7-7001-8000-000000000104",
			Type:     events.TicketOpenedV1,
			TicketID: "019535d9-3df7-7001-8000-000000000003",
			Subject:  "Link principal indisponível", Priority: "CRITICAL",
			OccurredAt: openedAt.Add(3 * time.Minute),
		},
	}
}
