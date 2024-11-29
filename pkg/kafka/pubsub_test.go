package kafka_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v3/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/subscriber"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
)

func kafkaBrokers() []string {
	brokers := os.Getenv("WATERMILL_TEST_KAFKA_BROKERS")
	if brokers != "" {
		return strings.Split(brokers, ",")
	}
	return []string{"localhost:9091", "localhost:9092", "localhost:9093", "localhost:9094", "localhost:9095"}
}

func newPubSub(t *testing.T, marshaler kafka.MarshalerUnmarshaler, consumerGroup string) (*kafka.Publisher, *kafka.Subscriber) {
	logger := watermill.NewStdLogger(true, true)

	var err error
	var publisher *kafka.Publisher

	retriesLeft := 5
	for {
		publisher, err = kafka.NewPublisher(kafka.PublisherConfig{
			Brokers:   kafkaBrokers(),
			Marshaler: marshaler,
		}, logger)
		if err == nil || retriesLeft == 0 {
			break
		}

		retriesLeft--
		fmt.Printf("cannot create kafka Publisher: %s, retrying (%d retries left)", err, retriesLeft)
		time.Sleep(time.Second * 2)
	}
	require.NoError(t, err)

	saramaConfig := kafka.DefaultSaramaSubscriberConfig()
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest

	saramaConfig.Admin.Timeout = time.Second * 30
	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	saramaConfig.ChannelBufferSize = 10240
	saramaConfig.Consumer.Group.Heartbeat.Interval = time.Millisecond * 500
	saramaConfig.Consumer.Group.Rebalance.Timeout = time.Second * 3
	saramaConfig.Consumer.IsolationLevel = sarama.ReadCommitted

	var subscriber *kafka.Subscriber

	retriesLeft = 5
	for {
		subscriber, err = kafka.NewSubscriber(
			kafka.SubscriberConfig{
				Brokers:               kafkaBrokers(),
				Unmarshaler:           marshaler,
				OverwriteSaramaConfig: saramaConfig,
				ConsumerGroup:         consumerGroup,
				InitializeTopicDetails: &sarama.TopicDetail{
					NumPartitions:     8,
					ReplicationFactor: 1,
				},
			},
			logger,
		)
		if err == nil || retriesLeft == 0 {
			break
		}

		retriesLeft--
		fmt.Printf("cannot create kafka Subscriber: %s, retrying (%d retries left)", err, retriesLeft)
		time.Sleep(time.Second * 2)
	}

	require.NoError(t, err)

	return publisher, subscriber
}

func newTransactionalPubSub(t *testing.T, marshaler kafka.MarshalerUnmarshaler, consumerGroup string) (*kafka.TransactionalPublisher, *kafka.Subscriber) {
	logger := watermill.NewStdLogger(true, true)

	var err error
	var publisher *kafka.TransactionalPublisher

	retriesLeft := 5
	for {
		publisher, err = kafka.NewTransactionalPublisher(kafka.TransactionalPublisherConfig{
			Brokers:   kafkaBrokers(),
			Marshaler: marshaler,
		}, logger)
		if err == nil || retriesLeft == 0 {
			break
		}

		retriesLeft--
		fmt.Printf("cannot create kafka Publisher: %s, retrying (%d retries left)", err, retriesLeft)
		time.Sleep(time.Second * 2)
	}
	require.NoError(t, err)

	saramaConfig := kafka.DefaultSaramaSubscriberConfig()
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest

	saramaConfig.Admin.Timeout = time.Second * 30
	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	saramaConfig.ChannelBufferSize = 10240
	saramaConfig.Consumer.Group.Heartbeat.Interval = time.Millisecond * 500
	saramaConfig.Consumer.Group.Rebalance.Timeout = time.Second * 3
	saramaConfig.Consumer.IsolationLevel = sarama.ReadCommitted

	var subscriber *kafka.Subscriber

	retriesLeft = 5
	for {
		subscriber, err = kafka.NewSubscriber(
			kafka.SubscriberConfig{
				Brokers:               kafkaBrokers(),
				Unmarshaler:           marshaler,
				OverwriteSaramaConfig: saramaConfig,
				ConsumerGroup:         consumerGroup,
				InitializeTopicDetails: &sarama.TopicDetail{
					NumPartitions:     8,
					ReplicationFactor: 1,
				},
			},
			logger,
		)
		if err == nil || retriesLeft == 0 {
			break
		}

		retriesLeft--
		fmt.Printf("cannot create kafka Subscriber: %s, retrying (%d retries left)", err, retriesLeft)
		time.Sleep(time.Second * 2)
	}

	require.NoError(t, err)

	return publisher, subscriber
}

func newExactlyOnceSubPub(t *testing.T, marshaler kafka.MarshalerUnmarshaler, consumerGroup string) (*kafka.Subscriber, *kafka.TransactionalPublisher) {
	logger := watermill.NewStdLogger(true, true)

	var err error
	var transactionalPublisher *kafka.TransactionalPublisher

	retriesLeft := 5
	for {
		transactionalPublisher, err = kafka.NewTransactionalPublisher(kafka.TransactionalPublisherConfig{
			Brokers:     kafkaBrokers(),
			Marshaler:   marshaler,
			ExactlyOnce: true,
		}, logger)
		if err == nil || retriesLeft == 0 {
			break
		}

		retriesLeft--
		fmt.Printf("cannot create kafka Publisher: %s, retrying (%d retries left)", err, retriesLeft)
		time.Sleep(time.Second * 2)
	}
	require.NoError(t, err)

	saramaConfig := kafka.DefaultSaramaSubscriberConfig()
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest

	saramaConfig.Admin.Timeout = time.Second * 30
	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	saramaConfig.ChannelBufferSize = 10240
	saramaConfig.Consumer.Group.Heartbeat.Interval = time.Millisecond * 500
	saramaConfig.Consumer.Group.Rebalance.Timeout = time.Second * 3
	saramaConfig.Consumer.IsolationLevel = sarama.ReadCommitted

	var subscriber *kafka.Subscriber

	retriesLeft = 5
	for {
		subscriber, err = kafka.NewSubscriber(
			kafka.SubscriberConfig{
				Brokers:               kafkaBrokers(),
				Unmarshaler:           marshaler,
				OverwriteSaramaConfig: saramaConfig,
				ConsumerGroup:         consumerGroup,
				InitializeTopicDetails: &sarama.TopicDetail{
					NumPartitions:     8,
					ReplicationFactor: 1,
				},
				ExactlyOnce: true,
			},
			logger,
		)
		if err == nil || retriesLeft == 0 {
			break
		}

		retriesLeft--
		fmt.Printf("cannot create kafka Subscriber: %s, retrying (%d retries left)", err, retriesLeft)
		time.Sleep(time.Second * 2)
	}

	require.NoError(t, err)

	return subscriber, transactionalPublisher
}

func generatePartitionKey(topic string, msg *message.Message) (string, error) {
	return msg.Metadata.Get("partition_key"), nil
}

func createPubSubWithConsumerGrup(t *testing.T, consumerGroup string) (message.Publisher, message.Subscriber) {
	return newPubSub(t, kafka.DefaultMarshaler{}, consumerGroup)
}

func createPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	return createPubSubWithConsumerGrup(t, "test")
}

func createTransactionalPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	return newTransactionalPubSub(t, kafka.DefaultMarshaler{}, "test")
}

func createTransactionalPubSubWithConsumerGroup(t *testing.T, consumerGroup string) (message.Publisher, message.Subscriber) {
	return newTransactionalPubSub(t, kafka.DefaultMarshaler{}, consumerGroup)
}

func createTransactionalPartitionedPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	return newTransactionalPubSub(t, kafka.NewWithPartitioningMarshaler(generatePartitionKey), "test")
}

func createPartitionedPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	return newPubSub(t, kafka.NewWithPartitioningMarshaler(generatePartitionKey), "test")
}

func createNoGroupPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	return newPubSub(t, kafka.DefaultMarshaler{}, "")
}

func TestPublishSubscribe(t *testing.T) {
	features := tests.Features{
		ConsumerGroups:      true,
		ExactlyOnceDelivery: false,
		GuaranteedOrder:     false,
		Persistent:          true,
	}

	tests.TestPubSub(
		t,
		features,
		createPubSub,
		createPubSubWithConsumerGrup,
	)
}

func TestTransactionalPublishSubscribe(t *testing.T) {
	features := tests.Features{
		ConsumerGroups:      true,
		ExactlyOnceDelivery: true,
		GuaranteedOrder:     true,
		Persistent:          true,
	}

	tests.TestPubSub(
		t,
		features,
		createTransactionalPartitionedPubSub,
		createTransactionalPubSubWithConsumerGroup,
	)
}

func TestPublishSubscribe_ordered(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long tests")
	}

	tests.TestPubSub(
		t,
		tests.Features{
			ConsumerGroups:      true,
			ExactlyOnceDelivery: false,
			GuaranteedOrder:     true,
			Persistent:          true,
		},
		createPartitionedPubSub,
		createPubSubWithConsumerGrup,
	)
}

func TestNoGroupSubscriber(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long tests")
	}

	tests.TestPubSub(
		t,
		tests.Features{
			ConsumerGroups:                   false,
			ExactlyOnceDelivery:              false,
			GuaranteedOrder:                  false,
			Persistent:                       true,
			NewSubscriberReceivesOldMessages: true,
		},
		createNoGroupPubSub,
		nil,
	)
}

func TestExactlyOncePubSub(t *testing.T) {
	const (
		numMessages = 500
		numWorkers  = 10
	)
	transactionalSub, transactionalPub := newExactlyOnceSubPub(t, kafka.NewWithPartitioningMarshaler(generatePartitionKey), "testExactlyOnce")
	inTopic := "inTopic_" + watermill.NewUUID()
	outTopic := "outTopic_" + watermill.NewUUID()
	require.NoError(t, transactionalSub.SubscribeInitialize(inTopic))
	require.NoError(t, transactionalSub.SubscribeInitialize(outTopic))

	router, err := message.NewRouter(message.RouterConfig{}, nil)
	require.NoError(t, err)

	router.AddHandler(
		"testExactlyOnce",
		inTopic,
		transactionalSub,
		outTopic,
		transactionalPub,
		func(msg *message.Message) ([]*message.Message, error) {
			return message.Messages{msg}, nil
		},
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		router.Run(ctx)
	}()

	pub, sub := newPubSub(t, kafka.NewWithPartitioningMarshaler(generatePartitionKey), "testExactlyOnceFrame")
	require.NoError(t, sub.SubscribeInitialize(outTopic))

	var messagesToPublish []*message.Message
	messageIDs := make(map[string]struct{}, numMessages)

	for i := 0; i < numMessages; i++ {
		id := watermill.NewUUID()
		messageIDs[id] = struct{}{}
		msg := message.NewMessage(id, []byte(`{"test": "test"}`))
		msg.Metadata.Set("partition_key", watermill.NewUUID())
		messagesToPublish = append(messagesToPublish, msg)
	}
	err = pub.Publish(inTopic, messagesToPublish...)
	require.NoError(t, err, "cannot publish message")

	messages, err := sub.Subscribe(context.Background(), outTopic)
	require.NoError(t, err)

	receivedMessages, all := subscriber.BulkRead(messages, len(messagesToPublish), time.Second*30)
	assert.True(t, all, "not all messages received")
	assert.Len(t, receivedMessages, numMessages, "number of received messages is not equal to number of published messages")

	for _, msg := range receivedMessages {
		_, ok := messageIDs[msg.UUID]
		require.True(t, ok, "received message with unexpected UUID: %s", msg.UUID)
		delete(messageIDs, msg.UUID)
	}
	assert.Empty(t, messageIDs, "did not find all expected message IDs")
	time.Sleep(time.Second * 5)

	require.NoError(t, pub.Close())

}

func TestCtxValues(t *testing.T) {
	pub, sub := newPubSub(t, kafka.DefaultMarshaler{}, "")
	topicName := "topic_" + watermill.NewUUID()

	var messagesToPublish []*message.Message

	for i := 0; i < 20; i++ {
		id := watermill.NewUUID()
		messagesToPublish = append(messagesToPublish, message.NewMessage(id, nil))
	}
	err := pub.Publish(topicName, messagesToPublish...)
	require.NoError(t, err, "cannot publish message")

	messages, err := sub.Subscribe(context.Background(), topicName)
	require.NoError(t, err)

	receivedMessages, all := subscriber.BulkReadWithDeduplication(messages, len(messagesToPublish), time.Second*10)
	require.True(t, all)

	expectedPartitionsOffsets := map[int32]int64{}
	for _, msg := range receivedMessages {
		partition, ok := kafka.MessagePartitionFromCtx(msg.Context())
		assert.True(t, ok)

		messagePartitionOffset, ok := kafka.MessagePartitionOffsetFromCtx(msg.Context())
		assert.True(t, ok)

		kafkaMsgTimestamp, ok := kafka.MessageTimestampFromCtx(msg.Context())
		assert.True(t, ok)
		assert.NotZero(t, kafkaMsgTimestamp)

		_, ok = kafka.MessageKeyFromCtx(msg.Context())
		assert.True(t, ok)

		if expectedPartitionsOffsets[partition] <= messagePartitionOffset {
			// kafka partition offset is offset of the last message + 1
			expectedPartitionsOffsets[partition] = messagePartitionOffset + 1
		}
	}
	assert.NotEmpty(t, expectedPartitionsOffsets)

	offsets, err := sub.PartitionOffset(topicName)
	require.NoError(t, err)
	assert.NotEmpty(t, offsets)

	assert.EqualValues(t, expectedPartitionsOffsets, offsets)

	require.NoError(t, pub.Close())
}

func readAfterRetries(messagesCh <-chan *message.Message, retriesN int, timeout time.Duration) (receivedMessage *message.Message, ok bool) {
	retries := 0

MessagesLoop:
	for retries <= retriesN {
		select {
		case msg, ok := <-messagesCh:
			if !ok {
				break MessagesLoop
			}

			if retries > 0 {
				msg.Ack()
				return msg, true
			}

			msg.Nack()
			retries++
		case <-time.After(timeout):
			break MessagesLoop
		}
	}

	return nil, false
}

func TestCtxValuesAfterRetry(t *testing.T) {
	pub, sub := newPubSub(t, kafka.DefaultMarshaler{}, "")
	topicName := "topic_" + watermill.NewUUID()

	var messagesToPublish []*message.Message

	id := watermill.NewUUID()
	messagesToPublish = append(messagesToPublish, message.NewMessage(id, nil))

	err := pub.Publish(topicName, messagesToPublish...)
	require.NoError(t, err, "cannot publish message")

	messages, err := sub.Subscribe(context.Background(), topicName)
	require.NoError(t, err)

	receivedMessage, ok := readAfterRetries(messages, 1, time.Second)
	assert.True(t, ok)

	expectedPartitionsOffsets := map[int32]int64{}
	partition, ok := kafka.MessagePartitionFromCtx(receivedMessage.Context())
	assert.True(t, ok)

	messagePartitionOffset, ok := kafka.MessagePartitionOffsetFromCtx(receivedMessage.Context())
	assert.True(t, ok)

	kafkaMsgTimestamp, ok := kafka.MessageTimestampFromCtx(receivedMessage.Context())
	assert.True(t, ok)
	assert.NotZero(t, kafkaMsgTimestamp)

	if expectedPartitionsOffsets[partition] <= messagePartitionOffset {
		// kafka partition offset is offset of the last message + 1
		expectedPartitionsOffsets[partition] = messagePartitionOffset + 1
	}
	assert.NotEmpty(t, expectedPartitionsOffsets)

	offsets, err := sub.PartitionOffset(topicName)
	require.NoError(t, err)
	assert.NotEmpty(t, offsets)

	assert.EqualValues(t, expectedPartitionsOffsets, offsets)

	require.NoError(t, pub.Close())
}
