package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	kafkago "github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafkago.Writer
}

func NewProducer(cfg Config) *Producer {
	return &Producer{
		writer: &kafkago.Writer{
			Addr:         kafkago.TCP(cfg.Brokers...),
			Topic:        cfg.Topic,
			Balancer:     &kafkago.Hash{},
			RequiredAcks: kafkago.RequireOne,
			Async:        false,
		},
	}
}

func (p *Producer) PublishJSON(ctx context.Context, key string, value any) error {
	body, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal kafka message: %w", err)
	}
	if err := p.writer.WriteMessages(ctx, kafkago.Message{
		Key:   []byte(key),
		Value: body,
	}); err != nil {
		return fmt.Errorf("write kafka message: %w", err)
	}
	return nil
}

func (p *Producer) Close() error {
	return p.writer.Close()
}
