package core

import "time"

type BrokerConfig struct {
	QueueName       string
	WALDir          string
	ListenAddr      string
	MaxRetries      int32
	DispatchTimeout time.Duration
}
