package dlqttl

import (
	"context"
	"log/slog"
	"time"

	"github.com/yashgupta/message-queue/internal/ackmgr"
	"github.com/yashgupta/message-queue/internal/queue"
)

// Scanner periodically expires DLQ messages older than retention.
type Scanner struct {
	manager    *queue.Manager
	ackManager *ackmgr.AckManager
	retention  time.Duration
	interval   time.Duration
	log        *slog.Logger
}

func NewScanner(manager *queue.Manager, ackManager *ackmgr.AckManager, retention, interval time.Duration, logger *slog.Logger) *Scanner {
	return &Scanner{
		manager:    manager,
		ackManager: ackManager,
		retention:  retention,
		interval:   interval,
		log:        logger.With("component", "dlq_ttl"),
	}
}

// Run starts the expiration loop. Blocks until ctx is cancelled.
// No-op when retention <= 0.
func (s *Scanner) Run(ctx context.Context) {
	if s.retention <= 0 {
		s.log.Info("dlq ttl disabled")
		return
	}

	s.log.Info("dlq ttl scanner started", "retention", s.retention, "interval", s.interval)
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.log.Info("dlq ttl scanner stopped")
			return
		case <-ticker.C:
			s.scan()
		}
	}
}

func (s *Scanner) scan() {
	cutoff := time.Now().Add(-s.retention)

	pending, err := s.manager.ExpireDLQOlderThan(s.retention)
	if err != nil {
		s.log.Error("expire pending dlq failed", "err", err)
	}

	inFlight, err2 := s.ackManager.ExpireDLQInFlight(cutoff)
	if err2 != nil {
		s.log.Error("expire in-flight dlq failed", "err", err2)
	}

	if pending > 0 || inFlight > 0 {
		s.log.Info("dlq messages expired", "pending", pending, "in_flight", inFlight)
	}
}
