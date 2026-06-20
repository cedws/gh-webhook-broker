package broker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/alecthomas/assert/v2"
)

type fakeGitHubClient struct {
	createHook func(Scope, []string, string) (Hook, error)
	updateHook func(Scope, int, []string, bool) (Hook, error)
	deleteHook func(Scope, int) error
	activate   func(Scope, int) (Hook, error)
	token      string
}

func (f fakeGitHubClient) CreateHook(scope Scope, events []string, secret string) (Hook, error) {
	return f.createHook(scope, events, secret)
}

func (f fakeGitHubClient) UpdateHook(scope Scope, id int, events []string, active bool) (Hook, error) {
	if f.updateHook == nil {
		return Hook{}, errors.New("unexpected UpdateHook call")
	}
	return f.updateHook(scope, id, events, active)
}

func (f fakeGitHubClient) DeleteHook(scope Scope, id int) error {
	if f.deleteHook == nil {
		return nil
	}
	return f.deleteHook(scope, id)
}

func (f fakeGitHubClient) Activate(scope Scope, id int) (Hook, error) {
	if f.activate == nil {
		return Hook{ID: id, Active: true}, nil
	}
	return f.activate(scope, id)
}

func (f fakeGitHubClient) Token() string {
	return f.token
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestAddSubscriber_RollsBackOnReconcileError(t *testing.T) {
	registry := NewRegistry(fakeGitHubClient{
		createHook: func(Scope, []string, string) (Hook, error) {
			return Hook{}, errors.New("boom")
		},
	}, "", testLogger())

	sub, err := NewSubscriber(SubscribeRequest{
		Events: []string{"push"},
		Scopes: []Scope{RepoScope("cedws", "gh-webhook-broker")},
	}, io.Discard, testLogger())
	assert.NoError(t, err)

	err = registry.AddSubscriber(context.Background(), sub)
	assert.Error(t, err)
	assert.Equal(t, 0, len(registry.subs))
}

func TestCreateHook_UsesActivatedHook(t *testing.T) {
	registry := NewRegistry(fakeGitHubClient{
		createHook: func(scope Scope, events []string, secret string) (Hook, error) {
			return Hook{ID: 42, Events: events, WsURL: ""}, nil
		},
		activate: func(scope Scope, id int) (Hook, error) {
			return Hook{ID: id, Active: true, Events: []string{"push"}, WsURL: "wss://example.test"}, nil
		},
	}, "", testLogger())

	hook, err := registry.createHook(RepoScope("cedws", "gh-webhook-broker"), []string{"push"})
	assert.NoError(t, err)
	assert.Equal(t, "wss://example.test", hook.WsURL)
	assert.True(t, hook.Active)
}
