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

type Subscriber struct {
	events map[string]struct{}
	scopes []Scope
	filter *Filter
	w      io.Writer
	mu     sync.Mutex
	log    *slog.Logger
}

func NewSubscriber(req SubscribeRequest, w io.Writer, log *slog.Logger) (*Subscriber, error) {
	f, err := CompileFilter(req.Match)
	if err != nil {
		return nil, err
	}

	return &Subscriber{
		events: eventSet(req.Events),
		scopes: req.Scopes,
		filter: f,
		w:      w,
		log:    log,
	}, nil
}

func (s *Subscriber) Wants(eventType string) bool {
	if len(s.events) == 0 {
		return true
	}

	if _, ok := s.events["*"]; ok {
		return true
	}

	_, ok := s.events[eventType]
	return ok
}

func (s *Subscriber) MatchScope(ev Event) bool {
	if len(s.scopes) == 0 {
		return true
	}

	return slices.Contains(s.scopes, ev.Scope)
}

func (s *Subscriber) Deliver(ev Event) error {
	if !s.MatchScope(ev) || !s.Wants(ev.Type) {
		return nil
	}

	ok, err := s.filter.Eval(ev)
	if err != nil {
		return err
	}

	if !ok {
		return nil
	}

	msg := Message{Event: &ev}
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

func (s *Subscriber) DesiredEvents() []string {
	if _, ok := s.events["*"]; ok {
		return []string{"*"}
	}

	return slices.Sorted(maps.Keys(s.events))
}

func (s *Subscriber) Scopes() []Scope { return s.scopes }
