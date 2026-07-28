package handlers

import (
	"context"
	"errors"
	"log"

	"mensageria-minimal/internal/events"
)

// ErrProviderUnavailable representa uma falha temporária do destino da mensagem.
var ErrProviderUnavailable = errors.New("provedor de notificação indisponível")

// Notification transforma eventos de Chamados em mensagens de comunicação.
// failTicketID habilita uma falha controlada sem alterar o contrato recebido.
type Notification struct {
	logger       *log.Logger
	failTicketID string
}

// NewNotification configura o Handler. failTicketID vazio mantém o fluxo normal;
// um valor conhecido faz o provedor falhar somente para aquele chamado.
func NewNotification(logger *log.Logger, failTicketID string) *Notification {
	return &Notification{logger: logger, failTicketID: failTicketID}
}

// Handle transforma o fato recebido em uma mensagem própria de Notificação.
// Ele não conhece partições, offsets, consumer groups nem política de retry.
func (handler *Notification) Handle(
	_ context.Context,
	event events.TicketEventV1,
) error {
	if event.TicketID == handler.failTicketID {
		return ErrProviderUnavailable
	}

	switch event.Type {
	case events.TicketOpenedV1:
		handler.logger.Printf(
			"NOTIFICAÇÃO preparada ticketID=%s mensagem=%q",
			event.TicketID, "Chamado aberto: "+event.Subject,
		)
	case events.TicketResolvedV1:
		handler.logger.Printf(
			"NOTIFICAÇÃO preparada ticketID=%s mensagem=%q",
			event.TicketID, "Chamado resolvido: "+event.Resolution,
		)
	}
	return nil
}
