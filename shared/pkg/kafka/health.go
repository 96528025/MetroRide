package kafka

import (
	"context"
	"fmt"

	kafkago "github.com/segmentio/kafka-go"
)

func Ping(ctx context.Context, brokers []string) error {
	if len(brokers) == 0 {
		return fmt.Errorf("no kafka brokers configured")
	}
	conn, err := kafkago.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return err
	}
	return conn.Close()
}
