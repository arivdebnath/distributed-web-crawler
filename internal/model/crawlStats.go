package model

type CrawlStats struct {
	TotalQueued           int     `json:"total_queued"`
	TotalCrawled          int     `json:"total_crawled"`
	TotalFailed           int     `json:"total_failed"`
	ActiveWorkers         int     `json:"active_workers"`
	AverageCrawlLatencyMs float64 `json:"average_crawl_latency_ms"`
	QueueDepth            int     `json:"queue_depth"`
	URLsPerSecond         float64 `json:"urls_per_second"`
}
