package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	proto "github.com/yashgupta/message-queue/proto/gen"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	addr := flag.String("addr", "localhost:50051", "broker address")
	queue := flag.String("queue", "orders", "target queue")
	count := flag.Int("count", 10, "number of messages to publish")
	interval := flag.Duration("interval", 0, "delay between publishes (e.g. 100ms)")
	payload := flag.String("payload", "msg", "payload prefix; each message becomes <prefix>-N")
	flag.Parse()

	log.SetFlags(0)

	conn, err := dialWithRetry(*addr)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer conn.Close()

	client := proto.NewBrokerClient(conn)

	fmt.Printf("publishing %d messages to queue=%s\n", *count, *queue)

	for i := 1; i <= *count; i++ {
		body := fmt.Sprintf("%s-%d", *payload, i)
		resp, err := client.Publish(context.Background(), &proto.PublishRequest{
			Queue:   *queue,
			Payload: []byte(body),
		})
		if err != nil {
			log.Fatalf("publish %d: %v", i, err)
		}
		ts := time.Now().Format("15:04:05.000")
		fmt.Printf("%s  published  queue=%-12s  payload=%-20s  id=%s\n",
			ts, *queue, body, resp.MessageId)

		if *interval > 0 {
			time.Sleep(*interval)
		}
	}

	fmt.Printf("done — published %d messages\n", *count)
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
