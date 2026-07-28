package events

import (
	"encoding/json"
	"time"
)

// DeadLetterV1 registra por que uma mensagem não pôde ser processada.
// Payload preserva a mensagem original para inspeção ou reprocessamento.
type DeadLetterV1 struct {
	SourceTopic     string          `json:"sourceTopic"`
	SourcePartition int32           `json:"sourcePartition"`
	SourceOffset    int64           `json:"sourceOffset"`
	Key             string          `json:"key"`
	Attempts        int             `json:"attempts"`
	Error           string          `json:"error"`
	FailedAt        time.Time       `json:"failedAt"`
	Payload         json.RawMessage `json:"payload"`
}
