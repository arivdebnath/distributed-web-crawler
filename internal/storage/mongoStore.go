package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/ariv/web-crawler/internal/config"
	"github.com/ariv/web-crawler/internal/model"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

type MongoStore struct {
	client     *mongo.Client
	collection *mongo.Collection
	log        *zap.Logger

	writeBuffer []*model.Page
	bufferMu    chan struct{}
	bufferSize  int
}

func NewMongoStore(cfg *config.Config, log *zap.Logger) (*MongoStore, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	clientOptions := options.Client().ApplyURI(cfg.MongoURI).SetMaxPoolSize(uint64(cfg.WorkerCount / 2)).SetMinPoolSize(5).SetServerSelectionTimeout(10 * time.Second)

	client, err := mongo.Connect(ctx, clientOptions)

	if err != nil {
		log.Error("mongodb connect error", zap.Error(err))
		return nil, fmt.Errorf("mongodb connect error: %w", err)
	}

	if err := client.Ping(ctx, nil); err != nil {
		log.Error("mongodb ping error", zap.Error(err))
		return nil, fmt.Errorf("mongodb ping error: %w", err)
	}

	collection := client.Database(cfg.MongoDB).Collection(cfg.MongoCollection)

	store := &MongoStore{
		client:     client,
		collection: collection,
		log:        log,
		bufferMu:   make(chan struct{}, 1),
		bufferSize: 100,
	}

	store.bufferMu <- struct{}{}

	if err := store.createIndexes(ctx); err != nil {
		log.Error("create indexes error", zap.Error(err))
		return nil, fmt.Errorf("create indexes error: %w", err)
	}

	log.Info("MongoDB connected",
		zap.String("uri", cfg.MongoURI),
		zap.String("database", cfg.MongoDB),
		zap.String("collection", cfg.MongoCollection),
	)

	return store, nil
}

func (store *MongoStore) createIndexes(ctx context.Context) error {
	indexes := []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "url", Value: 1}},
			Options: options.Index().SetUnique(true).SetName("url_unique"),
		},
		{
			Keys:    bson.D{{Key: "domain", Value: 1}},
			Options: options.Index().SetName("domain_idx"),
		},
		{
			Keys:    bson.D{{Key: "crawled_at", Value: -1}},
			Options: options.Index().SetName("crawled_at_idx"),
		},
		{
			Keys:    bson.D{{Key: "status", Value: 1}},
			Options: options.Index().SetName("status_idx"),
		},
		{
			Keys:    bson.D{{Key: "status_code", Value: 1}},
			Options: options.Index().SetName("status_code_idx"),
		},
		{
			Keys:    bson.D{{Key: "domain", Value: 1}, {Key: "status", Value: 1}},
			Options: options.Index().SetName("domain_status_idx"),
		},
	}

	_, err := store.collection.Indexes().CreateMany(ctx, indexes)
	return err
}
