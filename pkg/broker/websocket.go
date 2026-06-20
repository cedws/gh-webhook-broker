package broker

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

type wsEventReceived struct {
	Header http.Header `json:"Header"`
	Body   []byte      `json:"Body"`
}

type httpEventAck struct {
	Status int         `json:"Status"`
	Header http.Header `json:"Header"`
	Body   []byte      `json:"Body"`
}

type WebsocketReader struct {
	scope   Scope
	token   string
	wsURL   string
	log     *slog.Logger
	onEvent func(Event)
}

func NewWebsocketReader(scope Scope, token, wsURL string, log *slog.Logger, onEvent func(Event)) *WebsocketReader {
	return &WebsocketReader{
		scope:   scope,
		token:   token,
		wsURL:   wsURL,
		log:     log,
		onEvent: onEvent,
	}
}

func (r *WebsocketReader) Run(ctx context.Context) error {
	const maxRetries = 3
	backoff := 2 * time.Second

	for attempt := range maxRetries {
		if err := ctx.Err(); err != nil {
			return err
		}

		err := r.runOnce(ctx)
		if err == nil {
			return nil
		}

		if websocket.CloseStatus(err) == websocket.StatusNormalClosure {
			return nil
		}

		if websocket.CloseStatus(err) == websocket.StatusAbnormalClosure {
			r.log.Warn("websocket disconnected, retrying",
				"attempt", attempt+1,
				"backoff", backoff,
			)

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}

			backoff *= 2
			continue
		}

		return fmt.Errorf("websocket reader for %s: %w", r.scope, err)
	}

	return fmt.Errorf("websocket reader for %s exhausted retries", r.scope)
}

func (r *WebsocketReader) runOnce(ctx context.Context) error {
	conn, err := r.dial(ctx)
	if err != nil {
		return fmt.Errorf("dialing websocket: %w", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	r.log.Info("forwarding events from github")

	for {
		var ev wsEventReceived
		if err := wsjson.Read(ctx, conn, &ev); err != nil {
			return err
		}

		eventType := sanitise(ev.Header.Get("X-GitHub-Event"))
		r.log.Debug("received event", "type", eventType, "bytes", len(ev.Body))

		r.onEvent(Event{
			Type:    eventType,
			Scope:   r.scope,
			Payload: ev.Body,
		})

		ack := httpEventAck{
			Status: http.StatusOK,
			Header: make(http.Header),
			Body:   []byte("OK"),
		}
		if err := wsjson.Write(ctx, conn, &ack); err != nil {
			return fmt.Errorf("writing ack: %w", err)
		}
	}
}

func (r *WebsocketReader) dial(ctx context.Context) (*websocket.Conn, error) {
	conn, resp, err := websocket.Dial(ctx, r.wsURL, &websocket.DialOptions{
		HTTPHeader: http.Header{
			"Authorization": []string{r.token},
		},
	})
	if err != nil {
		if resp != nil {
			_ = resp.Body.Close()
			return nil, fmt.Errorf("ws dial %s: status %d: %w", r.wsURL, resp.StatusCode, err)
		}
		return nil, err
	}

	return conn, nil
}

func sanitise(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	if i := strings.IndexByte(s, '\r'); i >= 0 {
		return s[:i]
	}
	return s
}
