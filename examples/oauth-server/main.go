package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	core "github.com/aqaliarept/go-ddd-kit/pkg/core"
	redispkg "github.com/aqaliarept/go-ddd-kit/pkg/redis"
	"github.com/redis/go-redis/v9"

	"github.com/aqaliarept/go-ddd-kit/examples/oauth-server/domain"
)

func main() {
	cfg, err := LoadConfig()
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	opt, err := redis.ParseURL("redis://" + cfg.RedisAddr())
	if err != nil {
		log.Fatalf("failed to parse Redis URL: %v", err)
	}

	redisClient := redis.NewClient(opt)

	ctx := context.Background()
	if pingErr := redisClient.Ping(ctx).Err(); pingErr != nil {
		log.Fatalf("failed to connect to Redis: %v", pingErr)
	}

	repoFactory := redispkg.NewRepositoryFactory(redisClient)
	scope := core.NewConcurrentScope(repoFactory)

	clock := &domain.WallClock{}

	oauth := NewOAuthClient(cfg)
	worker := NewRefreshWorker(scope, oauth, cfg, clock)
	worker.Start()

	srv, err := NewServer(scope, oauth, worker, cfg, clock)
	if err != nil {
		log.Fatalf("failed to create server: %v", err)
	}

	httpServer := &http.Server{
		Addr:    ":" + cfg.ServerPort(),
		Handler: srv,
	}

	go func() {
		log.Printf("server starting on port %s", cfg.ServerPort())
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server error: %v", err)
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Println("shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("error shutting down server: %v", err)
	}

	worker.Shutdown()

	if err := redisClient.Close(); err != nil {
		log.Printf("error closing Redis client: %v", err)
	}

	log.Println("shutdown complete")
}
