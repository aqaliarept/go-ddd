package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"main/internal/server"

	core "github.com/aqaliarept/go-ddd-kit/pkg/core"
	redispkg "github.com/aqaliarept/go-ddd-kit/pkg/redis"
	"github.com/redis/go-redis/v9"
)

func main() {
	cfg, err := server.LoadConfig()
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	opt, err := redis.ParseURL("redis://" + cfg.RedisAddr())
	if err != nil {
		log.Fatalf("failed to parse Redis URL: %v", err)
	}

	redisClient := redis.NewClient(opt)

	ctx := context.Background()
	if err := redisClient.Ping(ctx).Err(); err != nil {
		log.Fatalf("failed to connect to Redis: %v", err)
	}

	repoFactory := redispkg.NewRepositoryFactory(redisClient)
	scope := core.NewConcurrentScope(repoFactory)

	oauth := server.NewOAuthClient(cfg)
	worker := server.NewRefreshWorker(scope, oauth, cfg)
	worker.Start()

	srv, err := server.NewServer(scope, oauth, worker, cfg)
	if err != nil {
		log.Fatalf("failed to create server: %v", err)
	}

	httpServer := &http.Server{
		Addr:    ":" + cfg.ServerPort(),
		Handler: srv,
	}

	go func() {
		log.Printf("server starting on port %s", cfg.ServerPort())
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
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
