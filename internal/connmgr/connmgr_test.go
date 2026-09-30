package connmgr_test

import (
	"testing"

	"github.com/yashgupta/message-queue/internal/connmgr"
)

func newTestConsumer(t *testing.T, queue string, prefetch int32) *connmgr.Consumer {
	t.Helper()
	consumer, err := connmgr.NewConsumer(queue, prefetch)
	if err != nil {
		t.Fatal(err)
	}
	return consumer
}

func TestRegistry_RegisterDeregister(t *testing.T) {
	registry := connmgr.NewRegistry()
	consumer := newTestConsumer(t, "orders", 5)

	registry.Register(consumer)

	if got := registry.EligibleConsumers("orders"); len(got) != 1 {
		t.Fatalf("want 1 consumer after register, got %d", len(got))
	}

	registry.Deregister(consumer.ID)

	if got := registry.EligibleConsumers("orders"); len(got) != 0 {
		t.Fatalf("want 0 consumers after deregister, got %d", len(got))
	}
}

func TestRegistry_MultipleConsumers(t *testing.T) {
	registry := connmgr.NewRegistry()

	ordersConsumer1 := newTestConsumer(t, "orders", 5)
	ordersConsumer2 := newTestConsumer(t, "orders", 5)
	paymentsConsumer := newTestConsumer(t, "payments", 5)

	registry.Register(ordersConsumer1)
	registry.Register(ordersConsumer2)
	registry.Register(paymentsConsumer)

	if got := registry.EligibleConsumers("orders"); len(got) != 2 {
		t.Fatalf("want 2 consumers for orders, got %d", len(got))
	}
	if got := registry.EligibleConsumers("payments"); len(got) != 1 {
		t.Fatalf("want 1 consumer for payments, got %d", len(got))
	}

	registry.Deregister(ordersConsumer1.ID)

	if got := registry.EligibleConsumers("orders"); len(got) != 1 {
		t.Fatalf("want 1 consumer for orders after deregister, got %d", len(got))
	}
	if got := registry.EligibleConsumers("payments"); len(got) != 1 {
		t.Fatal("payments consumer must not be affected by orders deregister")
	}
}

func TestRegistry_EligibleConsumers_RespectsPrefilter(t *testing.T) {
	registry := connmgr.NewRegistry()
	consumer := newTestConsumer(t, "orders", 2) // prefetch limit = 2
	registry.Register(consumer)

	// 0 in-flight → eligible
	if got := registry.EligibleConsumers("orders"); len(got) != 1 {
		t.Fatal("want eligible at 0 in-flight")
	}

	registry.IncrementInFlight(consumer.ID) // in-flight = 1

	// 1 in-flight → still eligible (1 < 2)
	if got := registry.EligibleConsumers("orders"); len(got) != 1 {
		t.Fatal("want eligible at 1 in-flight")
	}

	registry.IncrementInFlight(consumer.ID) // in-flight = 2

	// 2 in-flight → not eligible (2 == prefetch limit)
	if got := registry.EligibleConsumers("orders"); len(got) != 0 {
		t.Fatal("want ineligible when in-flight equals prefetch limit")
	}

	registry.DecrementInFlight(consumer.ID) // in-flight = 1

	// back to 1 in-flight → eligible again
	if got := registry.EligibleConsumers("orders"); len(got) != 1 {
		t.Fatal("want eligible again after decrement")
	}
}
