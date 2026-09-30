package connmgr

import "sync"

// Registry tracks all connected consumers, indexed by queue and by ID.
// byQueue is used by the dispatcher to find eligible consumers for a queue.
// byID is used by the Ack Manager to update in-flight counts by consumer ID.
type Registry struct {
	mu      sync.RWMutex
	byQueue map[string]map[string]*Consumer // queue → consumerID → Consumer
	byID    map[string]*Consumer             // consumerID → Consumer
}

func NewRegistry() *Registry {
	return &Registry{
		byQueue: make(map[string]map[string]*Consumer),
		byID:    make(map[string]*Consumer),
	}
}

func (r *Registry) Register(consumer *Consumer) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.byQueue[consumer.Queue]; !ok {
		r.byQueue[consumer.Queue] = make(map[string]*Consumer)
	}
	r.byQueue[consumer.Queue][consumer.ID] = consumer
	r.byID[consumer.ID] = consumer
}

func (r *Registry) Deregister(consumerID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	consumer, ok := r.byID[consumerID]
	if !ok {
		return
	}
	delete(r.byID, consumerID)
	delete(r.byQueue[consumer.Queue], consumerID)
}

// EligibleConsumers returns all consumers on a queue whose in-flight count
// is below their prefetch limit. Called by the dispatcher before each send.
func (r *Registry) EligibleConsumers(queue string) []*Consumer {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var eligible []*Consumer
	for _, consumer := range r.byQueue[queue] {
		if consumer.IsEligible() {
			eligible = append(eligible, consumer)
		}
	}
	return eligible
}

// IncrementInFlight increments the in-flight counter for the given consumer.
// Called by the dispatcher immediately before pushing a message.
func (r *Registry) IncrementInFlight(consumerID string) {
	r.mu.RLock()
	consumer := r.byID[consumerID]
	r.mu.RUnlock()

	if consumer != nil {
		consumer.IncrementInFlight()
	}
}

// DecrementInFlight decrements the in-flight counter for the given consumer.
// Called by the Ack Manager when a message is acknowledged or nacked.
func (r *Registry) DecrementInFlight(consumerID string) {
	r.mu.RLock()
	consumer := r.byID[consumerID]
	r.mu.RUnlock()

	if consumer != nil {
		consumer.DecrementInFlight()
	}
}
