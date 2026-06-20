package broker

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"slices"
	"sync"
)

type subscriber struct {
	events map[string]struct{}
	scopes []Scope
	filter *filter
	w      io.Writer
	mu     sync.Mutex
	log    *slog.Logger
}

func newSubscriber(req SubscribeRequest, w io.Writer, log *slog.Logger) (*subscriber, error) {
	f, err := compileFilter(req.Match)
	if err != nil {
		return nil, err
	}

	return &subscriber{
		events: eventSet(req.Events),
		scopes: req.Scopes,
		filter: f,
		w:      w,
		log:    log,
	}, nil
}

func (s *subscriber) wants(eventType string) bool {
	if len(s.events) == 0 {
		return true
	}

	if _, ok := s.events["*"]; ok {
		return true
	}

	_, ok := s.events[eventType]
	return ok
}

func (s *subscriber) matchScope(ev Event) bool {
	if len(s.scopes) == 0 {
		return true
	}

	return slices.Contains(s.scopes, ev.Scope)
}

func (s *subscriber) deliver(ev Event) error {
	if !s.matchScope(ev) || !s.wants(ev.Type) {
		return nil
	}

	ok, err := s.filter.eval(ev)
	if err != nil {
		return err
	}

	if !ok {
		return nil
	}

	msg := message{Event: &ev}
	data, err := json.Marshal(&msg)
	if err != nil {
		return fmt.Errorf("marshalling event: %w", err)
	}
	data = append(data, '\n')

	s.mu.Lock()
	defer s.mu.Unlock()

	_, err = s.w.Write(data)
	return err
}

func (s *subscriber) desiredEvents() []string {
	if _, ok := s.events["*"]; ok {
		return []string{"*"}
	}

	return slices.Sorted(maps.Keys(s.events))
}

func (s *subscriber) scopesFor() []Scope { return s.scopes }
