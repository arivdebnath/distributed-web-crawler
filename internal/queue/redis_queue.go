package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ariv/web-crawler/internal/config"
	"github.com/ariv/web-crawler/internal/model"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type RedisQueue struct {
	client      *redis.Client
	cfg         *config.Config
	log         *zap.Logger
	queueKey    string // crawl:queue
	visitedKey  string // crawl:visited
	failedKey   string // crawl:dql
	inflightKey string // crawl:inflight
	statsKey    string // crawl:stats
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

var enqueueScript = redis.NewScript(`
	local visited_key = KEYS[1]
	local queue_key = KEYS[2]
	local url = ARGV[1]
	local job_json = ARGV[2]

	if redis.call("SISMEMBER", visited_key, url) == 1 then
		return 0
	end

	redis.call("SADD", visited_key, url)
	redis.call("LPUSH", queue_key, job_json)
	return 1
`)

func (queue *RedisQueue) Enqueue(ctx context.Context, job *model.CrawlJob) (bool, error) {
	jobJson, err := json.Marshal(job)
	if err != nil {
		return false, fmt.Errorf("failed to marshal job to json: %w", err)
	}

	result, err := enqueueScript.Run(
		ctx,
		queue.client,
		[]string{queue.queueKey, queue.visitedKey},
		job.Url,
		jobJson,
	).Int()
	if err != nil {
		return false, fmt.Errorf("failed to enqueue job to redis: %w", err)
	}

	if result == 1 {
		queue.client.HIncrBy(ctx, queue.statsKey, "total_queued", 1)
	}

	return result == 1, nil
}
