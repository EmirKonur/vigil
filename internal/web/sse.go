package web

import (
	"fmt"
	"net/http"
	"sync"
	"time"
)

// Hub fans change notifications out to connected dashboards. Bursts are
// coalesced: a topic fires at most once per Debounce, carrying no payload,
// because clients re-fetch what they need.
type Hub struct {
	Debounce time.Duration

	mu      sync.Mutex
	subs    map[chan string]struct{}
	pending map[string]bool
	closed  bool
	done    chan struct{}
}

// NewHub returns a hub that coalesces events within debounce.
func NewHub(debounce time.Duration) *Hub {
	return &Hub{Debounce: debounce, subs: map[chan string]struct{}{}, pending: map[string]bool{}, done: make(chan struct{})}
}

// Publish signals that topic changed. It never blocks.
func (h *Hub) Publish(topic string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed || h.pending[topic] {
		return
	}
	h.pending[topic] = true
	time.AfterFunc(h.Debounce, func() { h.flush(topic) })
}

func (h *Hub) flush(topic string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.pending, topic)
	for ch := range h.subs {
		select {
		case ch <- topic:
		default:
			// A slow client misses an invalidation; the next one covers it.
		}
	}
}

func (h *Hub) subscribe() (chan string, func()) {
	ch := make(chan string, 8)
	h.mu.Lock()
	h.subs[ch] = struct{}{}
	h.mu.Unlock()
	return ch, func() {
		h.mu.Lock()
		delete(h.subs, ch)
		h.mu.Unlock()
	}
}

// Close ends every open stream, so HTTP shutdown does not wait on them.
func (h *Hub) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.closed {
		h.closed = true
		close(h.done)
	}
}

func (h *Handler) events(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Accel-Buffering", "no")
	ch, unsubscribe := h.opts.Hub.subscribe()
	defer unsubscribe()

	fmt.Fprint(w, "retry: 3000\n\n")
	flusher.Flush()
	keepalive := time.NewTicker(25 * time.Second)
	defer keepalive.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-h.opts.Hub.done:
			return
		case <-keepalive.C:
			fmt.Fprint(w, ": keepalive\n\n")
		case topic := <-ch:
			fmt.Fprintf(w, "event: %s\ndata: %d\n\n", topic, time.Now().UnixMilli())
		}
		flusher.Flush()
	}
}
