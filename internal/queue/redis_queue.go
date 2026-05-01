package queue

import (
	"context"
	"encoding/json"
	"errors"
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
	jobJSON, err := json.Marshal(job)
	if err != nil {
		return false, fmt.Errorf("failed to marshal job to json: %w", err)
	}

	result, err := enqueueScript.Run(
		ctx,
		queue.client,
		[]string{queue.queueKey, queue.visitedKey},
		job.Url,
		jobJSON,
	).Int()
	if err != nil {
		return false, fmt.Errorf("failed to enqueue job to redis: %w", err)
	}

	if result == 1 {
		queue.client.HIncrBy(ctx, queue.statsKey, "total_queued", 1)
	}

	return result == 1, nil
}

func (queue *RedisQueue) EnqueueBatch(ctx context.Context, jobs []*model.CrawlJob) (int, error) {
	enqueued := 0

	pipe := queue.client.Pipeline()
	cmds := make([]*redis.Cmd, len(jobs))

	for i, job := range jobs {
		jobJson, err := json.Marshal(job)
		if err != nil {
			return enqueued, fmt.Errorf("failed to marshal job to json: %w", err)
		}
		cmds[i] = enqueueScript.Run(ctx, pipe, []string{queue.queueKey, queue.visitedKey}, job.Url, string(jobJson))
	}

	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return enqueued, fmt.Errorf("failed to enqueue jobs to redis: %w", err)
	}

	for _, cmd := range cmds {
		if v, err := cmd.Int(); err == nil && v == 1 {
			enqueued++
		}
	}
	if enqueued > 0 {
		queue.client.HIncrBy(ctx, queue.statsKey, "total_queued", int64(enqueued))
	}
	return enqueued, nil
}

func (queue *RedisQueue) Dequeue(ctx context.Context) (*model.CrawlJob, error) {
	result, err := queue.client.BRPop(ctx, 5*time.Second, queue.queueKey).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to dequeue job from redis: %w", err)
	}
	var job model.CrawlJob
	error := json.Unmarshal([]byte(result[1]), &job)
	if error != nil {
		return nil, fmt.Errorf("failed to unmarshal job from redis: %w", err)
	}

	err = queue.client.ZAdd(ctx, queue.inflightKey, redis.Z{Score: float64(time.Now().Second()), Member: result[1]}).Err()

	if err != nil {
		_ = fmt.Errorf("failed to add job to inflight: %w", err)
	}
	return &job, nil
}

func (queue *RedisQueue) Acknowledge(ctx context.Context, job *model.CrawlJob) error {
	jobJSON, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("failed to marshal job to json: %w", err)
	}
	pipe := queue.client.Pipeline()
	pipe.ZRem(ctx, queue.inflightKey, string(jobJSON)).Result()
	pipe.HIncrBy(ctx, queue.statsKey, "total_crawled", 1)

	_, err = pipe.Exec(ctx)
	return err
}

func (queue *RedisQueue) Nack(ctx context.Context, job *model.CrawlJob, reason string) error {
	jobJSON, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("failed to marshal job to json: %w", err)
	}
	pipe := queue.client.Pipeline()
	pipe.ZRem(ctx, queue.inflightKey, string(jobJSON))

	if job.RetryCount < queue.cfg.MaxRetries {
		job.RetryCount++
		retryJob, _ := json.Marshal(job)
		pipe.LPush(ctx, queue.queueKey, string(retryJob))
		queue.log.Info("Re-queuing for Retry", zap.String("url", job.Url), zap.Int("retry_count", job.RetryCount))
	} else {
		pipe.LPush(ctx, queue.failedKey, string(jobJSON))
		pipe.HIncrBy(ctx, queue.statsKey, "total_failed", 1)
		queue.log.Warn("Job moved to DLQ", zap.String("url", job.Url), zap.String("reason", reason))
	}
	_, err = pipe.Exec(ctx)
	return err
}

//func (queue *RedisQueue) RecoverInFlight(ctx context.Context) error {
//	inflightJobs, err := queue.client.ZRange(ctx, queue.inflightKey, 0,)
//}
