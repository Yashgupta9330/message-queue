package api_test

import (
	"context"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/yashgupta/message-queue/internal/ackmgr"
	"github.com/yashgupta/message-queue/internal/api"
	"github.com/yashgupta/message-queue/internal/connmgr"
	"github.com/yashgupta/message-queue/internal/core"
	"github.com/yashgupta/message-queue/internal/dispatcher"
	"github.com/yashgupta/message-queue/internal/queue"
	proto "github.com/yashgupta/message-queue/proto/gen"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func discard() *slog.Logger { return slog.New(slog.DiscardHandler) }

func newTestClient(t *testing.T) (proto.BrokerClient, *queue.Manager) {
	t.Helper()

	manager, err := queue.NewManager(t.TempDir(), discard())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { manager.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	registry := connmgr.NewRegistry()
	ackMgr := ackmgr.NewAckManager(manager, registry, 3, discard())
	disp := dispatcher.NewDispatcher(manager, registry, ackMgr, discard())

	listener := bufconn.Listen(1 << 20)
	grpcServer := grpc.NewServer()
	proto.RegisterBrokerServer(grpcServer, api.NewServer(ctx, manager, registry, ackMgr, disp, discard()))
	go grpcServer.Serve(listener) //nolint:errcheck
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })

	return proto.NewBrokerClient(conn), manager
}

func TestPublish_ReturnsMessageID(t *testing.T) {
	client, _ := newTestClient(t)

	resp, err := client.Publish(context.Background(), &proto.PublishRequest{
		Queue:   "orders",
		Payload: []byte("hello"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.MessageId == "" {
		t.Fatal("expected non-empty message ID")
	}
}

func TestSubscribe_ConsumerReceivesMessage(t *testing.T) {
	client, _ := newTestClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, err := client.Subscribe(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if err := stream.Send(&proto.ConsumerMessage{
		Payload: &proto.ConsumerMessage_Subscribe{
			Subscribe: &proto.SubscribeRequest{Queue: "orders", PrefetchLimit: 1},
		},
	}); err != nil {
		t.Fatal(err)
	}

	resp, err := client.Publish(ctx, &proto.PublishRequest{
		Queue:   "orders",
		Payload: []byte("hello"),
	})
	if err != nil {
		t.Fatal(err)
	}

	received := make(chan *proto.SubscribeEvent, 1)
	go func() {
		event, err := stream.Recv()
		if err != nil {
			return
		}
		received <- event
	}()

	select {
	case event := <-received:
		if event.GetMessage() == nil {
			t.Fatal("expected message event")
		}
		if event.GetMessage().Id != resp.MessageId {
			t.Fatalf("want id=%s, got %s", resp.MessageId, event.GetMessage().Id)
		}
		if string(event.GetMessage().Payload) != "hello" {
			t.Fatalf("want payload=hello, got %s", event.GetMessage().Payload)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for message on stream")
	}
}

func TestSubscribe_DisconnectCleanly(t *testing.T) {
	client, _ := newTestClient(t)
	ctx, cancel := context.WithCancel(context.Background())

	stream, err := client.Subscribe(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if err := stream.Send(&proto.ConsumerMessage{
		Payload: &proto.ConsumerMessage_Subscribe{
			Subscribe: &proto.SubscribeRequest{Queue: "orders"},
		},
	}); err != nil {
		t.Fatal(err)
	}

	// Cancel the context, simulating a consumer disconnect.
	cancel()

	// Receiving after cancel must return an error, not panic.
	_, recvErr := stream.Recv()
	if recvErr == nil {
		t.Fatal("expected error after cancel, got nil")
	}
}

func TestPublish_MessageAppearsInQueue(t *testing.T) {
	client, manager := newTestClient(t)

	resp, err := client.Publish(context.Background(), &proto.PublishRequest{
		Queue:   "orders",
		Payload: []byte("hello"),
	})
	if err != nil {
		t.Fatal(err)
	}

	received := make(chan core.Message, 1)
	go func() {
		msg, _ := manager.Dequeue("orders")
		received <- msg
	}()

	select {
	case msg := <-received:
		if msg.ID != core.MessageID(resp.MessageId) {
			t.Fatalf("want id=%s, got %s", resp.MessageId, msg.ID)
		}
		if string(msg.Payload) != "hello" {
			t.Fatalf("want payload=hello, got %s", msg.Payload)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for message in queue")
	}
}
