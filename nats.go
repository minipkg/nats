package mp_nats

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"

	gonats "github.com/nats-io/nats.go"
)

type Config struct {
	Servers             string
	MaxReconnects       int
	ReconnectWaitInSecs time.Duration
	Login               string
	Password            string
}

type Nats interface {
	Conn() *gonats.Conn
	Js() gonats.JetStreamContext
	Close()

	IsStreamExists(name string) (bool, *gonats.StreamInfo, error)
	AddStreamIfNotExists(config *gonats.StreamConfig) (*gonats.StreamInfo, error)
	AddPushConsumerIfNotExists(streamName string, config *gonats.ConsumerConfig, msgHandler gonats.MsgHandler) (consumerInfo *gonats.ConsumerInfo, sub *gonats.Subscription, delConsumerAndSubscription func() error, err error)
	AddPullConsumerIfNotExists(streamName string, config *gonats.ConsumerConfig) (consumerInfo *gonats.ConsumerInfo, sub *gonats.Subscription, delConsumerAndSubscription func() error, err error)
}

type nats struct {
	conn *gonats.Conn
	js   gonats.JetStreamContext
}

var _ Nats = (*nats)(nil)

const (
	defaultMaxBytes        = 1024 * 1024
	defaultMaxRequestBatch = 1000
	defaultMaxWait         = 3 * time.Second
	defaultDuration        = 2 * time.Second
)

var defaultStreamConfig = &gonats.StreamConfig{
	Storage:    gonats.FileStorage,
	Retention:  gonats.LimitsPolicy,
	Discard:    gonats.DiscardOld,
	MaxMsgs:    -1,
	MaxMsgSize: -1,
	MaxAge:     0,
	MaxBytes:   defaultMaxBytes,
}

var defaultConsumerConfig = &gonats.ConsumerConfig{
	AckPolicy:       gonats.AckExplicitPolicy,
	AckWait:         time.Second,
	DeliverPolicy:   gonats.DeliverAllPolicy,
	MaxDeliver:      -1,
	MaxRequestBatch: defaultMaxRequestBatch,
}

func New(config *Config) (Nats, error) {
	var err error
	n := &nats{}
	options := []gonats.Option{
		gonats.MaxReconnects(config.MaxReconnects),
		gonats.ReconnectWait(config.ReconnectWaitInSecs),
		gonats.DisconnectErrHandler(func(nc *gonats.Conn, err error) {
			fmt.Printf("Got disconnected! Reason: %q\n", err)
		}),
		gonats.ReconnectHandler(func(nc *gonats.Conn) {
			fmt.Printf("Got reconnected to %v!\n", nc.ConnectedUrl())
		}),
		gonats.ClosedHandler(func(nc *gonats.Conn) {
			fmt.Printf("Connection closed. Reason: %q\n", nc.LastError())
		}),
	}

	if config.Login != "" && config.Password != "" {
		options = append(options, gonats.UserInfo(config.Login, config.Password))
	}

	n.conn, err = gonats.Connect(config.Servers, options...)
	if err != nil {
		return nil, err
	}

	n.js, err = n.conn.JetStream()
	if err != nil {
		return nil, err
	}

	return n, nil
}

func (n *nats) Conn() *gonats.Conn {
	return n.conn
}

func (n *nats) Js() gonats.JetStreamContext {
	return n.js
}

func (n *nats) Close() {
	n.conn.Close()
}

func (n *nats) IsStreamExists(name string) (bool, *gonats.StreamInfo, error) {
	info, err := n.js.StreamInfo(name)
	if err != nil {
		if errors.Is(err, gonats.ErrStreamNotFound) {
			return false, nil, nil
		}
		if err != nil {
			return false, nil, err
		}
	}
	return true, info, nil
}

func (n *nats) AddStreamIfNotExists(config *gonats.StreamConfig) (*gonats.StreamInfo, error) {
	isStreamExists, info, err := n.IsStreamExists(config.Name)
	if err != nil {
		return nil, err
	}

	c, err := getStreamConfig(config)
	if err != nil {
		return nil, err
	}

	if isStreamExists {
		return info, nil
	}

	return n.js.AddStream(c)
}

func (n *nats) AddPushConsumerIfNotExists(streamName string, config *gonats.ConsumerConfig, msgHandler gonats.MsgHandler) (consumerInfo *gonats.ConsumerInfo, sub *gonats.Subscription, delConsumerAndSubscription func() error, err error) {
	return n.addConsumerIfNotExists(streamName, config, msgHandler, true)
}

func (n *nats) AddPullConsumerIfNotExists(streamName string, config *gonats.ConsumerConfig) (consumerInfo *gonats.ConsumerInfo, sub *gonats.Subscription, delConsumerAndSubscription func() error, err error) {
	return n.addConsumerIfNotExists(streamName, config, nil, false)
}

func PullSubscriptionProcessing(ctx context.Context, subs *gonats.Subscription, msgHandler gonats.MsgHandler, duration time.Duration, requestBatch uint) error {
	if requestBatch == 0 {
		requestBatch = defaultMaxRequestBatch
	}

	for {
		select {
		case <-ctx.Done():
			break
		default:
		}

		bmsgs, err := subs.Fetch(int(requestBatch), gonats.MaxWait(defaultMaxWait))
		if err != nil {
			if !errors.Is(err, gonats.ErrTimeout) {
				return err
			}
			time.Sleep(duration)
		}
		for _, msg := range bmsgs {
			if err = msg.Ack(); err != nil {
				return err
			}
			msgHandler(msg)
		}
	}
	return nil
}

func (n *nats) addConsumerIfNotExists(streamName string, config *gonats.ConsumerConfig, msgHandler gonats.MsgHandler, isPushConsumer bool) (consumerInfo *gonats.ConsumerInfo, sub *gonats.Subscription, delConsumerAndSubscription func() error, err error) {
	if streamName == "" {
		return nil, nil, nil, errors.New("addConsumerIfNotExists error: streamName is empty.")
	}
	c, err := getConsumerConfig(config, isPushConsumer)
	if err != nil {
		return nil, nil, nil, err
	}
	js := n.js
	consumerInfo, err = js.ConsumerInfo(streamName, c.Name)
	if err != nil {
		if errors.Is(err, gonats.ErrConsumerNotFound) {
			consumerInfo, err = js.AddConsumer(streamName, c)
		}
		if err != nil {
			return nil, nil, nil, err
		}
	}

	if isPushConsumer {

		if c.DeliverGroup != "" {
			sub, err = js.QueueSubscribe(c.FilterSubject, c.DeliverGroup, msgHandler, gonats.Bind(streamName, c.Name))
		} else {
			sub, err = js.Subscribe(c.FilterSubject, msgHandler)
		}
	} else {
		sub, err = js.PullSubscribe(c.FilterSubject, c.Name, gonats.BindStream(streamName))
	}

	return consumerInfo, sub, func() (err error) {
		var errTxtB strings.Builder
		if err = sub.Unsubscribe(); err != nil {
			errTxtB.WriteString("sub.Unsubscribe error: " + err.Error() + "\n")
		}
		if err = js.DeleteConsumer(streamName, c.Name); err != nil {
			errTxtB.WriteString("js.DeleteConsumer error: " + err.Error() + "\n")
		}
		if errTxtB.Len() > 0 {
			err = errors.New(errTxtB.String())
		}
		return err
	}, err

}

func getStreamConfig(config *gonats.StreamConfig) (*gonats.StreamConfig, error) {
	if config.Name == "" {
		return nil, errors.New("getStreamConfig error: Name is empty.")
	}
	if config.Subjects == nil || len(config.Subjects) == 0 {
		return nil, errors.New("getStreamConfig error: Subjects is empty.")
	}
	c := &gonats.StreamConfig{}
	*c = *defaultStreamConfig
	copyStreamConfigValues(config, c)
	return c, nil
}

func getConsumerConfig(config *gonats.ConsumerConfig, isPushConsumer bool) (*gonats.ConsumerConfig, error) {
	if config.Name == "" {
		return nil, errors.New("getConsumerConfig error: Name is empty.")
	}
	if config.FilterSubject == "" {
		return nil, errors.New("getConsumerConfig error: FilterSubject is empty.")
	}
	if isPushConsumer && config.DeliverSubject == "" {
		config.DeliverSubject = config.Name
	}
	c := &gonats.ConsumerConfig{}
	*c = *defaultConsumerConfig
	copyConsumerConfigValues(config, c, isPushConsumer)
	return c, nil
}

func copyStreamConfigValues(source, target *gonats.StreamConfig) {
	if source.Name != "" {
		target.Name = source.Name
	}

	if source.Description != "" {
		target.Description = source.Description
	}

	if source.Subjects != nil {
		target.Subjects = source.Subjects
	}

	if source.Retention != 0 {
		target.Retention = source.Retention
	}

	if source.MaxConsumers != 0 {
		target.MaxConsumers = source.MaxConsumers
	}

	if source.MaxMsgs != 0 {
		target.MaxMsgs = source.MaxMsgs
	}

	if source.MaxBytes != 0 {
		target.MaxBytes = source.MaxBytes
	}

	if source.Discard != 0 {
		target.Discard = source.Discard
	}

	if source.DiscardNewPerSubject != false {
		target.DiscardNewPerSubject = source.DiscardNewPerSubject
	}

	if source.MaxAge != 0 {
		target.MaxAge = source.MaxAge
	}

	if source.MaxMsgsPerSubject != 0 {
		target.MaxMsgsPerSubject = source.MaxMsgsPerSubject
	}

	if source.MaxMsgSize != 0 {
		target.MaxMsgSize = source.MaxMsgSize
	}

	if source.Storage != 0 {
		target.Storage = source.Storage
	}

	if source.Replicas != 0 {
		target.Replicas = source.Replicas
	}

	if source.NoAck != false {
		target.NoAck = source.NoAck
	}

	if source.Template != "" {
		target.Template = source.Template
	}

	if source.Duplicates != 0 {
		target.Duplicates = source.Duplicates
	}

	if source.Placement != nil {
		target.Placement = source.Placement
	}

	if source.Mirror != nil {
		target.Mirror = source.Mirror
	}

	if source.Sources != nil {
		target.Sources = source.Sources
	}

	if source.Sealed != false {
		target.Sealed = source.Sealed
	}

	if source.DenyDelete != false {
		target.DenyDelete = source.DenyDelete
	}

	if source.DenyPurge != false {
		target.DenyPurge = source.DenyPurge
	}

	if source.AllowRollup != false {
		target.AllowRollup = source.AllowRollup
	}

	if source.RePublish != nil {
		target.RePublish = source.RePublish
	}

	if source.AllowDirect != false {
		target.AllowDirect = source.AllowDirect
	}

	if source.MirrorDirect != false {
		target.MirrorDirect = source.MirrorDirect
	}
}

func copyConsumerConfigValues(source, target *gonats.ConsumerConfig, isPushConsumer bool) {
	if source.Name != "" {
		target.Name = source.Name
	}

	if source.Durable != "" {
		target.Durable = source.Durable
	}

	if source.Description != "" {
		target.Description = source.Description
	}

	if source.DeliverPolicy != 0 {
		target.DeliverPolicy = source.DeliverPolicy
	}

	if source.OptStartSeq != 0 {
		target.OptStartSeq = source.OptStartSeq
	}

	if source.OptStartTime != nil {
		target.OptStartTime = source.OptStartTime
	}

	if source.AckPolicy != 0 {
		target.AckPolicy = source.AckPolicy
	}

	if source.AckWait != 0 {
		target.AckWait = source.AckWait
	}

	if source.MaxDeliver != 0 {
		target.MaxDeliver = source.MaxDeliver
	}

	if source.BackOff != nil {
		target.BackOff = source.BackOff
	}

	if source.FilterSubject != "" {
		target.FilterSubject = source.FilterSubject
	}

	if source.ReplayPolicy != 0 {
		target.ReplayPolicy = source.ReplayPolicy
	}

	if source.SampleFrequency != "" {
		target.SampleFrequency = source.SampleFrequency
	}

	if source.MaxAckPending != 0 {
		target.MaxAckPending = source.MaxAckPending
	}

	if source.InactiveThreshold != 0 {
		target.InactiveThreshold = source.InactiveThreshold
	}

	if source.Replicas != 0 {
		target.Replicas = source.Replicas
	}

	if source.MemoryStorage != false {
		target.MemoryStorage = source.MemoryStorage
	}

	if isPushConsumer {

		if source.DeliverSubject != "" {
			target.DeliverSubject = source.DeliverSubject
		}

		if source.DeliverGroup != "" {
			target.DeliverGroup = source.DeliverGroup
		}

		if source.FlowControl != false {
			target.FlowControl = source.FlowControl
		}

		if source.Heartbeat != 0 {
			target.Heartbeat = source.Heartbeat
		}

		if source.RateLimit != 0 {
			target.RateLimit = source.RateLimit
		}

		if source.HeadersOnly != false {
			target.HeadersOnly = source.HeadersOnly
		}

		target.MaxWaiting = 0
		target.MaxRequestExpires = 0
		target.MaxRequestBatch = 0
		target.MaxRequestMaxBytes = 0
	} else {

		if source.MaxWaiting != 0 {
			target.MaxWaiting = source.MaxWaiting
		}

		if source.MaxRequestExpires != 0 {
			target.MaxRequestExpires = source.MaxRequestExpires
		}

		if source.MaxRequestBatch != 0 {
			target.MaxRequestBatch = source.MaxRequestBatch
		}

		if source.MaxRequestMaxBytes != 0 {
			target.MaxRequestMaxBytes = source.MaxRequestMaxBytes
		}

		target.DeliverSubject = ""
		target.DeliverGroup = ""
		target.FlowControl = false
		target.Heartbeat = 0
		target.RateLimit = 0
		target.HeadersOnly = false
	}
}
