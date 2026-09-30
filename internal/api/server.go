package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/yashgupta/message-queue/internal/ackmgr"
	"github.com/yashgupta/message-queue/internal/connmgr"
	"github.com/yashgupta/message-queue/internal/core"
	"github.com/yashgupta/message-queue/internal/dispatcher"
	"github.com/yashgupta/message-queue/internal/queue"
	proto "github.com/yashgupta/message-queue/proto/gen"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server implements the BrokerServer gRPC interface.
type Server struct {
	proto.UnimplementedBrokerServer
	ctx        context.Context
	manager    *queue.Manager
	registry   *connmgr.Registry
	ackManager *ackmgr.AckManager
	disp       *dispatcher.Dispatcher
	log        *slog.Logger
	mu         sync.Mutex
	started    map[string]struct{} // queues with a running dispatcher goroutine
}

func NewServer(ctx context.Context, manager *queue.Manager, registry *connmgr.Registry, ackManager *ackmgr.AckManager, disp *dispatcher.Dispatcher, logger *slog.Logger) *Server {
	return &Server{
		ctx:        ctx,
		manager:    manager,
		registry:   registry,
		ackManager: ackManager,
		disp:       disp,
		log:        logger.With("component", "api"),
		started:    make(map[string]struct{}),
	}
}

func (s *Server) Publish(ctx context.Context, req *proto.PublishRequest) (*proto.PublishResponse, error) {
	messageID, err := newMessageID()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "generate id: %v", err)
	}

	headers := make([]core.Header, len(req.Headers))
	for i, h := range req.Headers {
		headers[i] = core.Header{Key: h.Key, Value: h.Value}
	}

	msg := core.Message{
		ID:          core.MessageID(messageID),
		Queue:       req.Queue,
		Payload:     req.Payload,
		Headers:     headers,
		EnqueueTime: time.Now(),
	}

	if err := s.manager.Enqueue(msg); err != nil {
		s.log.Error("enqueue failed", "queue", req.Queue, "msg_id", messageID, "err", err)
		return nil, status.Errorf(codes.Internal, "enqueue: %v", err)
	}

	s.log.Info("message published", "queue", req.Queue, "msg_id", messageID, "payload_bytes", len(req.Payload))
	return &proto.PublishResponse{MessageId: messageID}, nil
}

func (s *Server) Subscribe(stream grpc.BidiStreamingServer[proto.ConsumerMessage, proto.SubscribeEvent]) error {
	ctx := stream.Context()

	// First message must establish the subscription.
	firstMsg, err := stream.Recv()
	if err != nil {
		return status.Errorf(codes.Internal, "recv subscribe request: %v", err)
	}
	subscribeReq := firstMsg.GetSubscribe()
	if subscribeReq == nil {
		return status.Error(codes.InvalidArgument, "first message must be a SubscribeRequest")
	}

	queueName := subscribeReq.Queue
	prefetchLimit := subscribeReq.PrefetchLimit
	if prefetchLimit <= 0 {
		prefetchLimit = 1
	}

	consumer, err := connmgr.NewConsumer(queueName, prefetchLimit)
	if err != nil {
		return status.Errorf(codes.Internal, "create consumer: %v", err)
	}
	s.registry.Register(consumer)
	s.ensureDispatcher(queueName)

	s.log.Info("consumer connected", "consumer_id", consumer.ID, "queue", queueName, "prefetch", prefetchLimit)

	done := make(chan error, 2)

	// Send goroutine: reads messages pushed by the dispatcher and forwards them to the consumer stream.
	go func() {
		for {
			select {
			case msg := <-consumer.Send:
				if err := stream.Send(coreToProtoEvent(msg)); err != nil {
					done <- err
					return
				}
			case <-ctx.Done():
				done <- nil
				return
			}
		}
	}()

	// Recv goroutine: routes ACK/NACK from the consumer to the Ack Manager.
	go func() {
		for {
			consumerMsg, err := stream.Recv()
			if err != nil {
				if err == io.EOF || status.Code(err) == codes.Canceled {
					done <- nil
				} else {
					done <- err
				}
				return
			}
			if ack := consumerMsg.GetAck(); ack != nil {
				id := core.MessageID(ack.GetMessageId())
				if ack.GetOutcome() == proto.AckOutcome_NACK {
					_ = s.ackManager.Nack(id)
				} else {
					_ = s.ackManager.Ack(id)
				}
			}
		}
	}()

	streamErr := <-done

	// Deregister so the dispatcher stops selecting this consumer.
	s.registry.Deregister(consumer.ID)
	s.log.Info("consumer disconnected", "consumer_id", consumer.ID, "queue", queueName)

	// Drain messages buffered in consumer.Send that were never forwarded.
	// These are already registered in the AckManager; NACK them immediately
	// so they are requeued (or DLQ'd) rather than waiting for the timeout scanner.
	drained := 0
	for {
		select {
		case msg := <-consumer.Send:
			_ = s.ackManager.Nack(msg.ID)
			drained++
		default:
			if drained > 0 {
				s.log.Warn("drained undelivered messages on disconnect", "consumer_id", consumer.ID, "count", drained)
			}
			return streamErr
		}
	}
}

// ensureDispatcher starts a dispatcher goroutine for the given queue if one is not already running.
func (s *Server) ensureDispatcher(queueName string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.started[queueName]; ok {
		return
	}
	s.started[queueName] = struct{}{}
	go s.disp.Run(s.ctx, queueName)
}

func coreToProtoEvent(msg core.Message) *proto.SubscribeEvent {
	headers := make([]*proto.Header, len(msg.Headers))
	for i, h := range msg.Headers {
		headers[i] = &proto.Header{Key: h.Key, Value: h.Value}
	}
	return &proto.SubscribeEvent{
		Event: &proto.SubscribeEvent_Message{
			Message: &proto.Message{
				Id:              string(msg.ID),
				Queue:           msg.Queue,
				Payload:         msg.Payload,
				Headers:         headers,
				RetryCount:      msg.RetryCount,
				EnqueueTimeUnix: msg.EnqueueTime.UnixNano(),
			},
		},
	}
}

func newMessageID() (string, error) {
	randomBytes := make([]byte, 8)
	if _, err := rand.Read(randomBytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(randomBytes), nil
}
