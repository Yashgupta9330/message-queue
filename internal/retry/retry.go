package retry

import (
	"context"
	"log/slog"
	"time"

	"github.com/yashgupta/message-queue/internal/ackmgr"
)

// Scanner runs periodic timeout scans against the AckManager.
// Any in-flight message whose DispatchedAt is older than DispatchTimeout
// is treated as an implicit NACK: requeued (retries remaining) or sent to the DLQ.
type Scanner struct {
	ackManager      *ackmgr.AckManager
	interval        time.Duration
	dispatchTimeout time.Duration
	log             *slog.Logger
}

func NewScanner(ackManager *ackmgr.AckManager, interval time.Duration, dispatchTimeout time.Duration, logger *slog.Logger) *Scanner {
	return &Scanner{
		ackManager:      ackManager,
		interval:        interval,
		dispatchTimeout: dispatchTimeout,
		log:             logger.With("component", "scanner"),
	}
}

// Run starts the scan loop. Blocks until ctx is cancelled.
func (s *Scanner) Run(ctx context.Context) {
	s.log.Info("timeout scanner started", "interval", s.interval, "dispatch_timeout", s.dispatchTimeout)
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.log.Info("timeout scanner stopped")
			return
		case <-ticker.C:
			if err := s.ackManager.ScanAndTimeout(s.dispatchTimeout); err != nil {
				s.log.Error("scan failed", "err", err)
			}
		}
	}
}
