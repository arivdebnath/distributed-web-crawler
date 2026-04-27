package config

type Config struct {
	//Redis
	RedisAddr     string
	RedisPassword string
	RedisDB       int

	//Worker
	WorkerCount   int
	WorkerTimeout int
}
