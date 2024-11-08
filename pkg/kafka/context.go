package kafka

import (
	"context"
	"time"
)

type contextKey int

const (
	_ contextKey = iota
	timestampContextKey
	keyContextKey
	consumerDataContextKey
)

type ConsumerData struct {
	GroupID   string
	Topic     string
	Partition int32
	Offset    int64
}

func setConsumerDataToCtx(ctx context.Context, data ConsumerData) context.Context {
	return context.WithValue(ctx, consumerDataContextKey, data)
}

func ConsumerDataFromCtx(ctx context.Context) (ConsumerData, bool) {
	data, ok := ctx.Value(consumerDataContextKey).(ConsumerData)
	return data, ok
}

// MessagePartitionFromCtx returns Kafka partition of the consumed message
func MessagePartitionFromCtx(ctx context.Context) (int32, bool) {
	data, ok := ctx.Value(consumerDataContextKey).(ConsumerData)
	return data.Partition, ok
}

// MessagePartitionOffsetFromCtx returns Kafka partition offset of the consumed message
func MessagePartitionOffsetFromCtx(ctx context.Context) (int64, bool) {
	data, ok := ctx.Value(consumerDataContextKey).(ConsumerData)
	return data.Offset, ok
}

func setMessageTimestampToCtx(ctx context.Context, timestamp time.Time) context.Context {
	return context.WithValue(ctx, timestampContextKey, timestamp)
}

// MessageTimestampFromCtx returns Kafka internal timestamp of the consumed message
func MessageTimestampFromCtx(ctx context.Context) (time.Time, bool) {
	timestamp, ok := ctx.Value(timestampContextKey).(time.Time)
	return timestamp, ok
}

func setMessageKeyToCtx(ctx context.Context, key []byte) context.Context {
	return context.WithValue(ctx, keyContextKey, key)
}

// MessageKeyFromCtx returns Kafka internal key of the consumed message
func MessageKeyFromCtx(ctx context.Context) ([]byte, bool) {
	key, ok := ctx.Value(keyContextKey).([]byte)
	return key, ok
}
