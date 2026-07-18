package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestHealthHandler(t *testing.T) {
	hub := NewHub()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()

	healthHandler(hub)(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body map[string]any
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if body["status"] != "ok" {
		t.Errorf("expected status=ok, got %v", body["status"])
	}
}

func TestPublishHandler_NoAuth(t *testing.T) {
	hub := NewHub()
	body := `{"source":"test","type":"test.event"}`
	req := httptest.NewRequest(http.MethodPost, "/events/publish", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	// No JWT secret configured — auth is disabled, should succeed.
	publishHandler(hub, "", nil)(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", w.Code, w.Body.String())
	}
}

func TestPublishHandler_AuthRequired(t *testing.T) {
	hub := NewHub()
	body := `{"source":"test","type":"test.event"}`
	req := httptest.NewRequest(http.MethodPost, "/events/publish", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	publishHandler(hub, "secret", nil)(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", w.Code)
	}
}

func TestPublishHandler_ValidationError(t *testing.T) {
	hub := NewHub()
	body := `{"source":"","type":"test.event"}`
	req := httptest.NewRequest(http.MethodPost, "/events/publish", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	publishHandler(hub, "", nil)(w, req)

	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422, got %d", w.Code)
	}
}

func TestHubPubSub(t *testing.T) {
	hub := NewHub()
	ch, _ := hub.Subscribe("test-client")
	defer hub.Unsubscribe("test-client")

	hub.Publish(Event{ID: "1", Source: "test", Type: "test.event"})

	select {
	case e := <-ch:
		if e.ID != "1" {
			t.Errorf("expected id=1, got %s", e.ID)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event")
	}
}

func TestHubRingBufferReplay(t *testing.T) {
	hub := NewHub()

	for i := 0; i < 5; i++ {
		hub.Publish(Event{Source: "test", Type: "test.event"})
	}

	_, replay := hub.Subscribe("late-joiner")
	if len(replay) != 5 {
		t.Errorf("expected 5 replay events, got %d", len(replay))
	}
}

// TestHubConcurrentPublishUnsubscribeNoPanic stress-tests the fan-out against
// subscriber churn. Before the fix, Unsubscribe closed the channel while Publish
// sent to a snapshot of it outside the lock, so a disconnect racing a publish
// panicked with "send on closed channel". A panic in the publisher goroutine
// aborts the test process, so reaching the end without panicking is the
// assertion. Run with -race for maximum sensitivity.
func TestHubConcurrentPublishUnsubscribeNoPanic(t *testing.T) {
	hub := NewHub()
	done := make(chan struct{})

	go func() {
		for {
			select {
			case <-done:
				return
			default:
				hub.Publish(Event{Source: "test", Type: "test.event"})
			}
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < 500; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			id := fmt.Sprintf("client-%d", n)
			hub.Subscribe(id)
			hub.Unsubscribe(id)
		}(i)
	}
	wg.Wait()
	close(done)
}
