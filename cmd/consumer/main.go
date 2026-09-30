package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	proto "github.com/yashgupta/message-queue/proto/gen"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	addr := flag.String("addr", "localhost:50051", "broker address")
	queue := flag.String("queue", "orders", "queue to subscribe to")
	prefetch := flag.Int("prefetch", 5, "prefetch limit")
	delay := flag.Duration("delay", 0, "simulated processing time per message (e.g. 100ms)")
	outcome := flag.String("outcome", "ack", "message outcome: ack or nack")
	id := flag.String("id", "consumer", "label shown in log output")
	count := flag.Int("count", 0, "exit after receiving N messages (0 = run forever)")
	flag.Parse()

	log.SetFlags(0)

	conn, err := dialWithRetry(*addr)
	if err != nil {
		log.Fatalf("[%s] connect: %v", *id, err)
	}
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Fprintf(os.Stderr, "[%s] signal received, shutting down\n", *id)
		cancel()
	}()

	client := proto.NewBrokerClient(conn)
	stream, err := client.Subscribe(ctx)
	if err != nil {
		log.Fatalf("[%s] subscribe: %v", *id, err)
	}

	if err := stream.Send(&proto.ConsumerMessage{
		Payload: &proto.ConsumerMessage_Subscribe{
			Subscribe: &proto.SubscribeRequest{
				Queue:         *queue,
				PrefetchLimit: int32(*prefetch),
			},
		},
	}); err != nil {
		log.Fatalf("[%s] send subscribe request: %v", *id, err)
	}

	fmt.Printf("[%s] subscribed  queue=%-12s prefetch=%d  delay=%-8s  outcome=%s\n",
		*id, *queue, *prefetch, *delay, *outcome)

	received := 0
	for {
		event, err := stream.Recv()
		if err != nil {
			if err == io.EOF || ctx.Err() != nil {
				break
			}
			fmt.Fprintf(os.Stderr, "[%s] stream closed: %v\n", *id, err)
			break
		}

		msg := event.GetMessage()
		if msg == nil {
			continue
		}

		received++
		ts := time.Now().Format("15:04:05.000")
		fmt.Printf("[%s] %s  recv      id=%-22s  retry=%d  payload=%s\n",
			*id, ts, msg.Id[:min(8, len(msg.Id))]+"…", msg.RetryCount, string(msg.Payload))

		if *delay > 0 {
			time.Sleep(*delay)
		}

		ackOutcome := proto.AckOutcome_ACK
		label := "ACK"
		if *outcome == "nack" {
			ackOutcome = proto.AckOutcome_NACK
			label = "NACK"
		}

		if err := stream.Send(&proto.ConsumerMessage{
			Payload: &proto.ConsumerMessage_Ack{
				Ack: &proto.AckRequest{
					MessageId: msg.Id,
					Outcome:   ackOutcome,
				},
			},
		}); err != nil {
			fmt.Fprintf(os.Stderr, "[%s] send %s: %v\n", *id, label, err)
			break
		}

		ts = time.Now().Format("15:04:05.000")
		fmt.Printf("[%s] %s  %-4s      id=%-22s\n", *id, ts, label, msg.Id[:min(8, len(msg.Id))]+"…")

		if *count > 0 && received >= *count {
			fmt.Printf("[%s] reached limit (%d messages), exiting\n", *id, *count)
			// Give the server a moment to process the last ACK before we close.
			time.Sleep(200 * time.Millisecond)
			break
		}
	}

	fmt.Printf("[%s] done — total received: %d\n", *id, received)
}

func dialWithRetry(addr string) (*grpc.ClientConn, error) {
	var conn *grpc.ClientConn
	var err error
	for i := 0; i < 10; i++ {
		conn, err = grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			return conn, nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return nil, err
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
