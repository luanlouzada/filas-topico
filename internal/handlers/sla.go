// Package handlers contém reações de negócio que não conhecem o broker.
package handlers

import (
	"context"
	"fmt"
	"log"
	"time"

	"mensageria-minimal/internal/events"
)

// SLA reage aos fatos de Chamados como um consumidor independente.
type SLA struct {
	logger *log.Logger
}

// NewSLA recebe o logger usado para registrar as decisões tomadas pelo Handler.
func NewSLA(logger *log.Logger) *SLA {
	return &SLA{logger: logger}
}

// Handle traduz o evento publicado para uma decisão pertencente a SLA.
// A abertura calcula o prazo; a resolução conclui o acompanhamento.
func (handler *SLA) Handle(_ context.Context, event events.TicketEventV1) error {
	switch event.Type {
	case events.TicketOpenedV1:
		target, err := restorationTarget(event.Priority)
		if err != nil {
			return err
		}
		dueAt := event.OccurredAt.Add(target)
		handler.logger.Printf(
			"SLA iniciou ticketID=%s priority=%s target=%s dueAt=%s",
			event.TicketID, event.Priority, target, dueAt.Format(time.RFC3339),
		)
	case events.TicketResolvedV1:
		handler.logger.Printf(
			"SLA concluiu ticketID=%s occurredAt=%s",
			event.TicketID, event.OccurredAt.Format(time.RFC3339),
		)
	default:
		return fmt.Errorf("SLA não trata o evento %q", event.Type)
	}
	return nil
}

// restorationTarget mantém a política de prazo fora da infraestrutura Kafka.
func restorationTarget(priority string) (time.Duration, error) {
	switch priority {
	case "LOW":
		return 72 * time.Hour, nil
	case "NORMAL":
		return 24 * time.Hour, nil
	case "HIGH":
		return 4 * time.Hour, nil
	case "CRITICAL":
		return time.Hour, nil
	default:
		return 0, fmt.Errorf("prioridade desconhecida: %q", priority)
	}
}
