.PHONY: build test lint proto up down logs migrate fmt

build:
	go build -o bin/server ./cmd/server
	go build -o bin/worker ./cmd/worker

test:
	go test ./... -race -cover

lint:
	gofmt -l .
	go vet ./...

fmt:
	gofmt -w .

# Regenerates Go stubs from the proto definitions. Requires protoc and the
# protoc-gen-go / protoc-gen-go-grpc plugins on PATH.
proto:
	protoc \
		--go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		api/proto/taskqueue/v1/taskqueue.proto

up:
	docker compose -f deployments/docker-compose.yml up --build -d

down:
	docker compose -f deployments/docker-compose.yml down -v

logs:
	docker compose -f deployments/docker-compose.yml logs -f

migrate:
	docker compose -f deployments/docker-compose.yml exec -T postgres \
		psql -U taskqueue -d taskqueue -f /docker-entrypoint-initdb.d/0001_init.up.sql
