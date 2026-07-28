// O executável notification-consumer inicia um membro do grupo notification-service.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"mensageria-minimal/internal/handlers"
	"mensageria-minimal/internal/messaging"
)

// main monta o cliente Kafka, o Handler de Notificação e o loop de consumo.
// FAIL_TICKET_ID habilita a falha controlada usada para verificar retry e DLQ.
func main() {
	// O contexto é cancelado por Ctrl+C ou SIGTERM e desbloqueia PollRecords.
	ctx, stop := signal.NotifyContext(
		context.Background(), os.Interrupt, syscall.SIGTERM,
	)
	defer stop()

	logger := log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds)
	client, err := messaging.NewConsumer(
		messaging.Brokers(), messaging.NotificationConsumerGroup,
	)
	if err != nil {
		logger.Fatalf("criar consumer: %v", err)
	}
	defer client.Close()

	failTicketID := os.Getenv("FAIL_TICKET_ID")
	logger.Printf(
		"consumindo topic=%s group=%s failTicketID=%q; Ctrl+C encerra",
		messaging.TicketEventsTopic, messaging.NotificationConsumerGroup, failTicketID,
	)
	runner := messaging.NewConsumerRunner(
		client,
		messaging.NotificationConsumerGroup,
		handlers.NewNotification(logger, failTicketID),
		logger,
	)
	if err := runner.Run(ctx); err != nil {
		logger.Fatalf("executar consumer: %v", err)
	}
}
