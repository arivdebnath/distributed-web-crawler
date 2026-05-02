package config

import "time"

type Config struct {
	//Redis
	RedisAddr     string
	RedisPassword string
	RedisDB       int

	//Worker
	WorkerCount   int
	WorkerTimeout int

	//Job
	MaxRetries      int
	InflightTimeout time.Duration
}
