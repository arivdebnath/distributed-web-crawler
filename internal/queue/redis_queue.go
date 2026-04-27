package queue

import (
	"time"

	"github.com/ariv/web-crawler/internal/config"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type RedisQueue struct {
	client *redis.Client
	cfg    *config.Config
	log    *zap.Logger
}

func newRedisQueue(cfg *config.Config, log *zap.Logger) (*RedisQueue, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         cfg.RedisAddr,
		Password:     cfg.RedisPassword,
		DB:           cfg.RedisDB,
		PoolSize:     cfg.WorkerCount + 10,
		DialTimeout:  5 * time.Second,
		MinIdleConns: 5,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.Ping(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}
	log.Info("Successfully connected to Redis", zap.String("address", cfg.RedisAddr))
	
	return &RedisQueue{
		client: client,
		cfg:    cfg,
		log:    log,
	}, nil
}
