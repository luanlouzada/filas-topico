package messaging

import (
	"errors"
	"testing"
)

// TestHandleWithRetrySucceedsOnThirdAttempt prova que o primeiro sucesso encerra o retry.
func TestHandleWithRetrySucceedsOnThirdAttempt(t *testing.T) {
	t.Parallel()

	calls := 0
	attempts, err := handleWithRetry(
		t.Context(), 3, 0,
		func() error {
			calls++
			if calls < 3 {
				return errors.New("dependência indisponível")
			}
			return nil
		},
		func(int, error) {},
	)
	if err != nil {
		t.Fatalf("handleWithRetry() erro = %v", err)
	}
	if attempts != 3 || calls != 3 {
		t.Fatalf("tentativas = %d, chamadas = %d; esperado = 3, 3", attempts, calls)
	}
}

// TestHandleWithRetryReturnsLastError prova o limite antes do envio para a DLQ.
func TestHandleWithRetryReturnsLastError(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("falha permanente")
	attempts, err := handleWithRetry(
		t.Context(), 3, 0,
		func() error { return wantErr },
		func(int, error) {},
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("erro = %v; esperado = %v", err, wantErr)
	}
	if attempts != 3 {
		t.Fatalf("tentativas = %d; esperado = 3", attempts)
	}
}
