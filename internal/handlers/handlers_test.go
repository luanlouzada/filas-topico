package handlers

import (
	"errors"
	"io"
	"log"
	"testing"
	"time"

	"mensageria-minimal/internal/events"
)

// TestNotificationFailureInjection verifica a falha configurável que aciona retry e DLQ.
func TestNotificationFailureInjection(t *testing.T) {
	t.Parallel()

	handler := NewNotification(log.New(io.Discard, "", 0), "ticket-fail")
	event := events.TicketEventV1{
		EventID: "event-1", Type: events.TicketOpenedV1,
		TicketID: "ticket-fail", Subject: "Sem internet", Priority: "HIGH",
		OccurredAt: time.Now(),
	}

	err := handler.Handle(t.Context(), event)
	if !errors.Is(err, ErrProviderUnavailable) {
		t.Fatalf("erro = %v; esperado = %v", err, ErrProviderUnavailable)
	}
}

// TestSLAAcceptsKnownPriority confirma que uma prioridade do contrato é traduzida.
func TestSLAAcceptsKnownPriority(t *testing.T) {
	t.Parallel()

	handler := NewSLA(log.New(io.Discard, "", 0))
	event := events.TicketEventV1{
		EventID: "event-1", Type: events.TicketOpenedV1,
		TicketID: "ticket-1", Subject: "Sem internet", Priority: "HIGH",
		OccurredAt: time.Now(),
	}

	if err := handler.Handle(t.Context(), event); err != nil {
		t.Fatalf("Handle() erro = %v", err)
	}
}
