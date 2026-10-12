// Command server runs the task queue coordinator: it accepts gRPC
// connections from workers (Lease/Heartbeat/Ack/Nack/Enqueue, see
// api/proto/taskqueue/v1/taskqueue.proto), owns the in-memory priority
// schedule (internal/queue) and lease table (internal/lease), and
// persists durable state to Postgres with Redis as a wake-up fast path.
package main

import (
	"context"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"

	"github.com/tanaydeps/distributed-task-queue/internal/config"
	"github.com/tanaydeps/distributed-task-queue/internal/lease"
	"github.com/tanaydeps/distributed-task-queue/internal/metrics"
	"github.com/tanaydeps/distributed-task-queue/internal/queue"
	"github.com/tanaydeps/distributed-task-queue/internal/storage"
)

func main() {
	cfgPath := flag.String("config", "configs/server.yaml", "path to server config file")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("server: loading config: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	pgPool, err := pgxpool.New(ctx, cfg.Postgres.DSN)
	if err != nil {
		log.Fatalf("server: connecting to postgres: %v", err)
	}
	defer pgPool.Close()
	taskStore := storage.NewPostgresStore(pgPool)

	redisClient := redis.NewClient(&redis.Options{Addr: cfg.Redis.Addr, DB: cfg.Redis.DB})
	defer redisClient.Close()
	notifier := storage.NewRedisStore(redisClient)
	_ = notifier // wired into the Lease RPC handler; see internal/server (TODO below)

	schedule := queue.New()
	leaseMgr := lease.NewManager(cfg.Lease.TTL)

	registry := prometheus.NewRegistry()
	registry.MustRegister(metrics.Collectors()...)

	go runReaper(ctx, taskStore, leaseMgr, schedule)
	go serveMetrics(cfg.Metrics.Addr, registry)

	lis, err := net.Listen("tcp", cfg.Server.GRPCAddr)
	if err != nil {
		log.Fatalf("server: listening on %s: %v", cfg.Server.GRPCAddr, err)
	}

	grpcServer := grpc.NewServer()
	// TODO: register the generated TaskQueueServer implementation here
	// once `make proto` has generated taskqueue_grpc.pb.go from
	// api/proto/taskqueue/v1/taskqueue.proto. The handler wiring itself
	// (Lease pulling from internal/queue.PriorityQueue, Heartbeat/Ack/Nack
	// delegating to internal/lease.Manager and internal/retry.Policy) is
	// implemented in internal/server, pending that codegen step.
	// taskqueuev1.RegisterTaskQueueServer(grpcServer, server.New(taskStore, notifier, schedule, leaseMgr))

	log.Printf("server: gRPC listening on %s", cfg.Server.GRPCAddr)
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("server: grpc serve: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("server: shutting down")
	grpcServer.GracefulStop()
}

// runReaper periodically reclaims leases abandoned by crashed workers and
// returns the underlying tasks to the schedulable pool, implementing the
// system's automatic crash recovery.
func runReaper(ctx context.Context, store *storage.PostgresStore, leaseMgr *lease.Manager, schedule *queue.PriorityQueue) {
	ticker := time.NewTicker(lease.DefaultTTL / 2)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			expired, err := store.ListExpiredLeases(ctx)
			if err != nil {
				log.Printf("reaper: listing expired leases: %v", err)
				continue
			}
			for _, t := range expired {
				metrics.LeasesReclaimedTotal.WithLabelValues(t.Queue).Inc()
				schedule.Push(t)
			}
		}
	}
}

func serveMetrics(addr string, registry *prometheus.Registry) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	log.Printf("server: metrics listening on %s/metrics", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("server: metrics server: %v", err)
	}
}
