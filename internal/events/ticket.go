// Package events contém a linguagem publicada pelo produtor.
// Os consumidores dependem desse contrato, não do modelo interno de Chamados.
package events

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// TicketEventType identifica o fato ocorrido sem depender de nomes de funções.
type TicketEventType string

const (
	// TicketOpenedV1 informa que um chamado iniciou seu ciclo de vida.
	TicketOpenedV1 TicketEventType = "TicketOpenedV1"
	// TicketResolvedV1 informa que um chamado recebeu uma resolução.
	TicketResolvedV1 TicketEventType = "TicketResolvedV1"
)

// Erros de validação identificam qual parte do contrato V1 está incompleta.
var (
	ErrEventIDRequired    = errors.New("eventId é obrigatório")
	ErrTicketIDRequired   = errors.New("ticketId é obrigatório")
	ErrEventTypeInvalid   = errors.New("tipo de evento inválido")
	ErrOccurredAtRequired = errors.New("occurredAt é obrigatório")
	ErrSubjectRequired    = errors.New("subject é obrigatório para TicketOpenedV1")
	ErrPriorityInvalid    = errors.New("priority inválida para TicketOpenedV1")
	ErrResolutionRequired = errors.New("resolution é obrigatória para TicketResolvedV1")
)

// TicketEventV1 é o contrato publicado no tópico support.ticket-events.v1.
// Type determina quais campos específicos são obrigatórios em cada fato.
type TicketEventV1 struct {
	EventID    string          `json:"eventId"`
	Type       TicketEventType `json:"type"`
	TicketID   string          `json:"ticketId"`
	Subject    string          `json:"subject,omitempty"`
	Priority   string          `json:"priority,omitempty"`
	Resolution string          `json:"resolution,omitempty"`
	OccurredAt time.Time       `json:"occurredAt"`
}

// Validate rejeita mensagens que não representam um fato completo do contrato V1.
func (event TicketEventV1) Validate() error {
	if strings.TrimSpace(event.EventID) == "" {
		return ErrEventIDRequired
	}
	if strings.TrimSpace(event.TicketID) == "" {
		return ErrTicketIDRequired
	}
	if event.OccurredAt.IsZero() {
		return ErrOccurredAtRequired
	}

	switch event.Type {
	case TicketOpenedV1:
		if strings.TrimSpace(event.Subject) == "" {
			return ErrSubjectRequired
		}
		switch event.Priority {
		case "LOW", "NORMAL", "HIGH", "CRITICAL":
			return nil
		default:
			return ErrPriorityInvalid
		}
	case TicketResolvedV1:
		if strings.TrimSpace(event.Resolution) == "" {
			return ErrResolutionRequired
		}
		return nil
	default:
		return ErrEventTypeInvalid
	}
}

// Marshal valida antes de serializar para não publicar uma mensagem inválida.
func (event TicketEventV1) Marshal() ([]byte, error) {
	if err := event.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(event)
}

// ParseTicketEvent desserializa o JSON e só entrega eventos válidos ao Handler.
func ParseTicketEvent(payload []byte) (TicketEventV1, error) {
	var event TicketEventV1
	if err := json.Unmarshal(payload, &event); err != nil {
		return TicketEventV1{}, fmt.Errorf("decodificar TicketEventV1: %w", err)
	}
	if err := event.Validate(); err != nil {
		return TicketEventV1{}, fmt.Errorf("validar TicketEventV1: %w", err)
	}
	return event, nil
}
