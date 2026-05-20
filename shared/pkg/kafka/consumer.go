package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader *kafkago.Reader
}

func NewConsumer(cfg Config) *Consumer {
	return &Consumer{
		reader: kafkago.NewReader(kafkago.ReaderConfig{
			Brokers:        cfg.Brokers,
			Topic:          cfg.Topic,
			GroupID:        cfg.GroupID,
			MinBytes:       1,
			MaxBytes:       1e6,
			MaxWait:        ReadTimeout(),
			CommitInterval: time.Second,
			StartOffset:    kafkago.FirstOffset,
		}),
	}
}

func (c *Consumer) ReadJSON(ctx context.Context, out any) (kafkago.Message, error) {
	message, err := c.reader.ReadMessage(ctx)
	if err != nil {
		return kafkago.Message{}, fmt.Errorf("read kafka message: %w", err)
	}
	if err := json.Unmarshal(message.Value, out); err != nil {
		return message, fmt.Errorf("decode kafka message: %w", err)
	}
	return message, nil
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
