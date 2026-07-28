.PHONY: infra-up infra-down reset topics test test-integration publish run-sla run-notification run-notification-failing

FAIL_TICKET_ID ?= 019535d9-3df7-7001-8000-000000000003

infra-up:
	docker compose up -d --wait

infra-down:
	docker compose down

reset:
	docker compose down -v

topics:
	docker compose exec redpanda rpk topic list --brokers localhost:9092

test:
	go test ./...

test-integration:
	go test -tags=integration -count=1 ./integration

publish:
	go run ./cmd/publisher

run-sla:
	go run ./cmd/sla-consumer

run-notification:
	go run ./cmd/notification-consumer

run-notification-failing:
	FAIL_TICKET_ID=$(FAIL_TICKET_ID) go run ./cmd/notification-consumer
