package model

import (
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type CrawlStatus string

const (
	StatusPending   CrawlStatus = "pending"
	StatusCrawling  CrawlStatus = "crawling"
	StatusCompleted CrawlStatus = "completed"
	StatusFailed    CrawlStatus = "failed"
)

type Page struct {
	ID            primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	URL           string             `bson:"url"     json:"url"`
	Domain        string             `bson:"domain" json:"domain"`
	Title         string             `bson:"title" json:"title"`
	StatusCode    int                `bson:"statusCode" json:"statusCode"`
	ContentType   string             `bson:"contentType" json:"contentType"`
	ContentLength int                `bson:"contentLength" json:"contentLength"`
	Links         []string           `bson:"links" json:"links"`
	Depth         int                `bson:"depth" json:"depth"`
	LatencyMs     int64              `bson:"latencyMs" json:"latencyMs"`
	WorkerId      string             `bson:"workerId" json:"workerId"`
	Status        CrawlStatus        `bson:"status" json:"status"`
	ErrMsg        string             `bson:"errMsg" json:"errMsg"`
	RetryCount    int                `bson:"retryCount" json:"retryCount"`
}
