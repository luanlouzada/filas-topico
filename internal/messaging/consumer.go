package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"mensageria-minimal/internal/events"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Handler contém a reação de negócio e não conhece tópico, partição ou offset.
type Handler interface {
	Handle(context.Context, events.TicketEventV1) error
}

// Consumer coordena leitura, tentativas, DLQ e commit do offset.
type Consumer struct {
	client      *kgo.Client
	group       string
	dlqTopic    string
	handler     Handler
	maxAttempts int
	retryDelay  time.Duration
	now         func() time.Time
	logger      *log.Logger
}

// NewConsumerRunner define a política de falha do consumidor: três tentativas,
// intervalo de 250 ms e encaminhamento final para a DLQ.
func NewConsumerRunner(
	client *kgo.Client,
	group string,
	handler Handler,
	logger *log.Logger,
) *Consumer {
	return &Consumer{
		client: client, group: group, dlqTopic: TicketEventsDLQTopic,
		handler: handler, maxAttempts: 3, retryDelay: 250 * time.Millisecond,
		now: time.Now, logger: logger,
	}
}

// Run consulta o broker enquanto o contexto estiver ativo. Cada iteração recebe
// no máximo um registro e libera o rebalance somente após concluir seu processamento.
func (consumer *Consumer) Run(ctx context.Context) error {
	for {
		// PollRecords consulta o broker e devolve no máximo um registro nesta
		// chamada. O contexto cancelado interrompe a espera.
		fetches := consumer.client.PollRecords(ctx, 1)
		if ctx.Err() != nil {
			return nil
		}
		// Errors reúne falhas de busca por broker, tópico ou partição.
		if errs := fetches.Errors(); len(errs) > 0 {
			return fmt.Errorf("consultar broker: %v", errs)
		}

		// Records achata os lotes recebidos em uma sequência de *kgo.Record.
		for _, record := range fetches.Records() {
			err := consumer.processRecord(ctx, record)
			// Libera um eventual rebalance bloqueado por BlockRebalanceOnPoll
			// somente depois de terminar a mensagem atual.
			consumer.client.AllowRebalance()
			if err != nil {
				return err
			}
		}
	}
}

// processRecord separa o ciclo de uma mensagem: decodificar, executar a reação,
// encaminhar falhas permanentes e só então confirmar o offset.
func (consumer *Consumer) processRecord(ctx context.Context, record *kgo.Record) error {
	event, err := events.ParseTicketEvent(record.Value)
	if err != nil {
		// Uma nova tentativa não corrige JSON inválido ou contrato incompleto.
		// Por isso a mensagem segue diretamente para a DLQ.
		consumer.logger.Printf(
			"grupo=%s mensagem inválida topic=%s partition=%d offset=%d erro=%v",
			consumer.group, record.Topic, record.Partition, record.Offset, err,
		)
		if dlqErr := consumer.publishDeadLetter(ctx, record, 1, err); dlqErr != nil {
			return dlqErr
		}
		return consumer.commit(ctx, record)
	}

	attempts, err := handleWithRetry(
		ctx, consumer.maxAttempts, consumer.retryDelay,
		func() error { return consumer.handler.Handle(ctx, event) },
		func(attempt int, err error) {
			consumer.logger.Printf(
				"grupo=%s tentativa=%d/%d ticketID=%s erro=%v",
				consumer.group, attempt, consumer.maxAttempts, event.TicketID, err,
			)
		},
	)
	if err != nil {
		if dlqErr := consumer.publishDeadLetter(ctx, record, attempts, err); dlqErr != nil {
			return dlqErr
		}
		consumer.logger.Printf(
			"grupo=%s enviou ticketID=%s para dlq=%s",
			consumer.group, event.TicketID, consumer.dlqTopic,
		)
	} else {
		consumer.logger.Printf(
			"grupo=%s confirmou topic=%s partition=%d offset=%d key=%s",
			consumer.group, record.Topic, record.Partition, record.Offset, record.Key,
		)
	}

	// O offset só avança após sucesso ou após a cópia na DLQ.
	return consumer.commit(ctx, record)
}

// publishDeadLetter preserva o diagnóstico e o payload original em outro tópico.
// Se essa publicação falhar, o registro original permanece sem commit.
func (consumer *Consumer) publishDeadLetter(
	ctx context.Context,
	record *kgo.Record,
	attempts int,
	cause error,
) error {
	deadLetter := events.DeadLetterV1{
		SourceTopic: record.Topic, SourcePartition: record.Partition,
		SourceOffset: record.Offset, Key: string(record.Key), Attempts: attempts,
		Error: cause.Error(), FailedAt: consumer.now().UTC(),
		Payload: append(json.RawMessage(nil), record.Value...),
	}
	// As cópias impedem que buffers pertencentes ao client sejam reutilizados
	// enquanto o novo registro está sendo produzido.
	payload, err := json.Marshal(deadLetter)
	if err != nil {
		return fmt.Errorf("serializar dead letter: %w", err)
	}

	// A DLQ também é um tópico Kafka. O novo Record mantém a chave do registro
	// original e leva no Value o envelope DeadLetterV1 serializado.
	dlqRecord := &kgo.Record{
		Topic: consumer.dlqTopic,
		Key:   append([]byte(nil), record.Key...),
		Value: payload,
	}
	if err := consumer.client.ProduceSync(ctx, dlqRecord).FirstErr(); err != nil {
		return fmt.Errorf("publicar na DLQ: %w", err)
	}
	return nil
}

// commit registra no consumer group até onde esta partição foi processada.
func (consumer *Consumer) commit(ctx context.Context, record *kgo.Record) error {
	// CommitRecords confirma para o Consumer Group o próximo offset a consumir
	// nesta partição. Se o processo reiniciar, continua depois deste registro.
	if err := consumer.client.CommitRecords(ctx, record); err != nil {
		return fmt.Errorf("confirmar offset: %w", err)
	}
	return nil
}

// handleWithRetry repete somente a reação de negócio. A leitura da mensagem e
// o commit continuam sob responsabilidade do Consumer.
func handleWithRetry(
	ctx context.Context,
	maxAttempts int,
	delay time.Duration,
	handle func() error,
	onFailure func(int, error),
) (int, error) {
	if maxAttempts < 1 {
		return 0, errors.New("maxAttempts deve ser maior que zero")
	}

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := handle(); err == nil {
			return attempt, nil
		} else {
			lastErr = err
			onFailure(attempt, err)
		}

		if attempt == maxAttempts {
			break
		}
		// O timer permite interromper o intervalo de retry quando o contexto é cancelado.
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return attempt, ctx.Err()
		case <-timer.C:
		}
	}
	return maxAttempts, lastErr
}
