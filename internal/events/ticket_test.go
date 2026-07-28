package events

import (
	"errors"
	"testing"
	"time"
)

// TestTicketEventValidate cobre as combinações mínimas aceitas pelo contrato.
func TestTicketEventValidate(t *testing.T) {
	t.Parallel()

	now := time.Date(2027, time.March, 15, 9, 30, 0, 0, time.UTC)
	tests := []struct {
		name    string
		event   TicketEventV1
		wantErr error
	}{
		{
			name: "chamado aberto válido",
			event: TicketEventV1{
				EventID: "event-1", Type: TicketOpenedV1, TicketID: "ticket-1",
				Subject: "Sem internet", Priority: "HIGH", OccurredAt: now,
			},
		},
		{
			name: "chamado resolvido válido",
			event: TicketEventV1{
				EventID: "event-2", Type: TicketResolvedV1, TicketID: "ticket-1",
				Resolution: "Roteador reiniciado", OccurredAt: now,
			},
		},
		{
			name: "prioridade inválida",
			event: TicketEventV1{
				EventID: "event-3", Type: TicketOpenedV1, TicketID: "ticket-2",
				Subject: "Sem sinal", Priority: "URGENT", OccurredAt: now,
			},
			wantErr: ErrPriorityInvalid,
		},
		{
			name: "resolução ausente",
			event: TicketEventV1{
				EventID: "event-4", Type: TicketResolvedV1, TicketID: "ticket-2",
				OccurredAt: now,
			},
			wantErr: ErrResolutionRequired,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			err := test.event.Validate()
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("Validate() erro = %v; esperado = %v", err, test.wantErr)
			}
		})
	}
}

// TestTicketEventMarshalAndParse prova a ida e a volta entre contrato e JSON.
func TestTicketEventMarshalAndParse(t *testing.T) {
	t.Parallel()

	want := TicketEventV1{
		EventID: "event-1", Type: TicketOpenedV1, TicketID: "ticket-1",
		Subject: "Sem internet", Priority: "HIGH",
		OccurredAt: time.Date(2027, time.March, 15, 9, 30, 0, 0, time.UTC),
	}
	payload, err := want.Marshal()
	if err != nil {
		t.Fatalf("Marshal() erro = %v", err)
	}

	got, err := ParseTicketEvent(payload)
	if err != nil {
		t.Fatalf("ParseTicketEvent() erro = %v", err)
	}
	if got != want {
		t.Fatalf("evento recebido = %#v; esperado = %#v", got, want)
	}
}
