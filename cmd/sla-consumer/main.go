// O executável sla-consumer inicia um membro do grupo sla-service.
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

// main cria um membro do grupo sla-service e o mantém ativo até Ctrl+C ou
// SIGTERM. O contexto comunica esse encerramento ao loop de consumo.
func main() {
	// O contexto é cancelado por Ctrl+C ou SIGTERM e desbloqueia PollRecords.
	ctx, stop := signal.NotifyContext(
		context.Background(), os.Interrupt, syscall.SIGTERM,
	)
	defer stop()

	logger := log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds)
	client, err := messaging.NewConsumer(messaging.Brokers(), messaging.SLAConsumerGroup)
	if err != nil {
		logger.Fatalf("criar consumer: %v", err)
	}
	defer client.Close()

	logger.Printf(
		"consumindo topic=%s group=%s; Ctrl+C encerra",
		messaging.TicketEventsTopic, messaging.SLAConsumerGroup,
	)
	runner := messaging.NewConsumerRunner(
		client, messaging.SLAConsumerGroup, handlers.NewSLA(logger), logger,
	)
	if err := runner.Run(ctx); err != nil {
		logger.Fatalf("executar consumer: %v", err)
	}
}
