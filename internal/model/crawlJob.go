package model

// job struct present in queue
type CrawlJob struct {
	Url        string `json:"url"`
	Depth      int    `json:"depth"`
	RetryCount int    `json:"retry_count"`
	ParentUrl  string `json:"parent_url"`
}
