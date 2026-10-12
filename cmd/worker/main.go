// Command worker connects to the server over gRPC, leases tasks for a
// configured set of queues, executes them, and acks/nacks the result. It
// runs a background heartbeat loop per in-flight task so a hung or
// crashed worker process is detected by the server's lease reaper
// (internal/lease) rather than requiring a separate health check system.
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/tanaydeps/distributed-task-queue/internal/config"
	"github.com/tanaydeps/distributed-task-queue/internal/metrics"
)

func main() {
	cfgPath := flag.String("config", "configs/worker.yaml", "path to worker config file")
	queueName := flag.String("queue", "default", "queue to consume from")
	concurrency := flag.Int("concurrency", 4, "number of concurrent task executions")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("worker: loading config: %v", err)
	}

	workerID := uuid.New().String()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	conn, err := grpc.NewClient(cfg.Server.GRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("worker: dialing server at %s: %v", cfg.Server.GRPCAddr, err)
	}
	defer conn.Close()
	// TODO: client := taskqueuev1.NewTaskQueueClient(conn) once the proto
	// stubs are generated (see cmd/server/main.go TODO for the matching
	// server-side registration). The loop below sketches the intended
	// control flow against that client.

	registry := prometheus.NewRegistry()
	registry.MustRegister(metrics.Collectors()...)
	go serveMetrics(cfg.Metrics.Addr, registry)

	log.Printf("worker: %s starting, queue=%s concurrency=%d", workerID, *queueName, *concurrency)

	sem := make(chan struct{}, *concurrency)
	for {
		select {
		case <-ctx.Done():
			log.Printf("worker: %s shutting down", workerID)
			return
		case sem <- struct{}{}:
			go func() {
				defer func() { <-sem }()
				runOnce(ctx, workerID, *queueName)
			}()
		}
	}
}

// runOnce leases a single task, runs it, and reports the outcome. The
// heartbeat ticker keeps the lease alive for the duration of execution;
// if the process dies mid-task the ticker simply stops and the server's
// reaper reclaims the lease after internal/lease.DefaultTTL elapses.
func runOnce(ctx context.Context, workerID, queueName string) {
	start := time.Now()
	defer func() {
		metrics.TaskLatencySeconds.WithLabelValues(queueName).Observe(time.Since(start).Seconds())
	}()

	// TODO: resp, err := client.Lease(ctx, &taskqueuev1.LeaseRequest{Queue: queueName, WorkerId: workerID, WaitTimeoutSeconds: 20})
	// TODO: start a ticker calling client.Heartbeat every internal/lease.DefaultTTL/3
	// TODO: dispatch resp.Task.Payload to the registered handler for resp.Task.Queue
	// TODO: on success call client.Ack; on failure call client.Nack with the error
	time.Sleep(100 * time.Millisecond) // placeholder cadence until the RPC loop above is generated-code-complete
}

func serveMetrics(addr string, registry *prometheus.Registry) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("worker: metrics server: %v", err)
	}
}
