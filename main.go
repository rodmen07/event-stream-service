package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

// ────────────────────────────────────────────────────────────────────────────
// Domain types
// ────────────────────────────────────────────────────────────────────────────

// Event is the canonical unit published to the stream.
type Event struct {
	ID        string          `json:"id"`
	Source    string          `json:"source"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	Timestamp string          `json:"timestamp"`
}

type publishRequest struct {
	Source  string          `json:"source"`
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

type errorResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// ────────────────────────────────────────────────────────────────────────────
// Auth
// ────────────────────────────────────────────────────────────────────────────

func validateBearer(authHeader, secret string) error {
	if secret == "" {
		return nil // auth disabled in dev
	}
	parts := strings.SplitN(authHeader, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "bearer") {
		return errors.New("missing or malformed Authorization header")
	}
	token, err := jwt.Parse(parts[1], func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
		}
		return []byte(secret), nil
	})
	if err != nil || !token.Valid {
		return errors.New("invalid or expired token")
	}
	return nil
}

// ────────────────────────────────────────────────────────────────────────────
// CORS
// ────────────────────────────────────────────────────────────────────────────

func setCORSHeaders(w http.ResponseWriter, r *http.Request, allowed []string) {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return
	}
	for _, o := range allowed {
		if o == "*" || o == origin {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			return
		}
	}
}

func parseOrigins(raw string) []string {
	var out []string
	for _, o := range strings.Split(raw, ",") {
		if s := strings.TrimSpace(o); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// ────────────────────────────────────────────────────────────────────────────
// Helpers
// ────────────────────────────────────────────────────────────────────────────

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, errorResponse{Code: code, Message: message})
}

// ────────────────────────────────────────────────────────────────────────────
// Handlers
// ────────────────────────────────────────────────────────────────────────────

func healthHandler(hub *Hub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"status":           "ok",
			"connected_clients": hub.ClientCount(),
		})
	}
}

func publishHandler(hub *Hub, jwtSecret string, allowedOrigins []string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		setCORSHeaders(w, r, allowedOrigins)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		if err := validateBearer(r.Header.Get("Authorization"), jwtSecret); err != nil {
			writeError(w, http.StatusUnauthorized, "UNAUTHORIZED", err.Error())
			return
		}

		var req publishRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "BAD_REQUEST", "invalid JSON body")
			return
		}
		if strings.TrimSpace(req.Source) == "" {
			writeError(w, http.StatusUnprocessableEntity, "VALIDATION_ERROR", "source is required")
			return
		}
		if strings.TrimSpace(req.Type) == "" {
			writeError(w, http.StatusUnprocessableEntity, "VALIDATION_ERROR", "type is required")
			return
		}

		e := Event{
			ID:      uuid.New().String(),
			Source:  req.Source,
			Type:    req.Type,
			Payload: req.Payload,
		}
		hub.Publish(e)

		slog.Info("event published", "id", e.ID, "source", e.Source, "type", e.Type)
		writeJSON(w, http.StatusAccepted, map[string]string{"id": e.ID})
	}
}

func streamHandler(hub *Hub, allowedOrigins []string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		setCORSHeaders(w, r, allowedOrigins)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		// SSE requires the client to support flushing.
		flusher, ok := w.(http.Flusher)
		if !ok {
			writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "streaming not supported")
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("X-Accel-Buffering", "no") // disable nginx buffering

		id := uuid.New().String()
		ch, replay := hub.Subscribe(id)
		defer hub.Unsubscribe(id)

		slog.Info("client connected", "id", id, "replay_count", len(replay))

		// Replay recent history so the client isn't starting cold.
		for _, e := range replay {
			if err := writeSSEEvent(w, e); err != nil {
				return
			}
		}
		flusher.Flush()

		// Stream live events until the client disconnects.
		for {
			select {
			case <-r.Context().Done():
				slog.Info("client disconnected", "id", id)
				return
			case e, ok := <-ch:
				if !ok {
					return
				}
				if err := writeSSEEvent(w, e); err != nil {
					return
				}
				flusher.Flush()
			}
		}
	}
}

func writeSSEEvent(w http.ResponseWriter, e Event) error {
	data, err := json.Marshal(e)
	if err != nil {
		return err
	}
	// Emit as unnamed data message so browsers can catch all events
	// with a single addEventListener('message') listener. The event
	// type is already present inside the JSON payload.
	_, err = fmt.Fprintf(w, "id: %s\ndata: %s\n\n", e.ID, data)
	return err
}

// ────────────────────────────────────────────────────────────────────────────
// Entry point
// ────────────────────────────────────────────────────────────────────────────

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8085"
	}
	jwtSecret := os.Getenv("AUTH_JWT_SECRET")
	allowedOrigins := parseOrigins(os.Getenv("ALLOWED_ORIGINS"))

	hub := NewHub()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", healthHandler(hub))
	mux.HandleFunc("OPTIONS /events/publish", publishHandler(hub, jwtSecret, allowedOrigins))
	mux.HandleFunc("POST /events/publish", publishHandler(hub, jwtSecret, allowedOrigins))
	mux.HandleFunc("OPTIONS /events/stream", streamHandler(hub, allowedOrigins))
	mux.HandleFunc("GET /events/stream", streamHandler(hub, allowedOrigins))

	slog.Info("event-stream-service starting", "port", port)
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		slog.Error("server error", "err", err)
		os.Exit(1)
	}
}
