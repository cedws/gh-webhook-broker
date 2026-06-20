package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"slices"
	"sync"

	"github.com/cli/go-gh/v2/pkg/api"
	"golang.org/x/sync/errgroup"
)

type managedHook struct {
	scope     Scope
	hook      Hook
	cancel    context.CancelFunc
	log       *slog.Logger
	mu        sync.Mutex
	desired   map[string]struct{}
	activated bool
	owned     bool
}

type githubHooksClient interface {
	CreateHook(Scope, []string, string) (Hook, error)
	UpdateHook(Scope, int, []string, bool) (Hook, error)
	DeleteHook(Scope, int) error
	Activate(Scope, int) (Hook, error)
	Token() string
}

type Registry struct {
	gh     githubHooksClient
	secret string
	log    *slog.Logger

	mu    sync.Mutex
	hooks map[Scope]*managedHook

	subsMu sync.RWMutex
	subs   map[*Subscriber]struct{}
}

func NewRegistry(gh githubHooksClient, secret string, log *slog.Logger) *Registry {
	return &Registry{
		gh:     gh,
		secret: secret,
		log:    log,
		hooks:  make(map[Scope]*managedHook),
		subs:   make(map[*Subscriber]struct{}),
	}
}

func (r *Registry) AddSubscriber(ctx context.Context, sub *Subscriber) error {
	r.subsMu.Lock()
	r.subs[sub] = struct{}{}
	r.subsMu.Unlock()

	if err := r.reconcileAll(ctx, sub.Scopes()); err != nil {
		r.subsMu.Lock()
		delete(r.subs, sub)
		r.subsMu.Unlock()
		return err
	}

	return nil
}

func (r *Registry) RemoveSubscriber(ctx context.Context, sub *Subscriber) error {
	r.subsMu.Lock()
	delete(r.subs, sub)
	r.subsMu.Unlock()

	return r.reconcileAll(ctx, sub.Scopes())
}

func (r *Registry) reconcileAll(ctx context.Context, scopes []Scope) error {
	for i := range scopes {
		if err := r.reconcile(ctx, &scopes[i]); err != nil {
			return err
		}
	}
	return nil
}

func (r *Registry) subscribersFor(scope *Scope) []*Subscriber {
	r.subsMu.RLock()
	defer r.subsMu.RUnlock()

	var out []*Subscriber
	for sub := range r.subs {
		subScopes := sub.Scopes()
		if scope == nil || len(subScopes) == 0 {
			out = append(out, sub)
			continue
		}
		if slices.Contains(subScopes, *scope) {
			out = append(out, sub)
		}
	}
	return out
}

func (r *Registry) desiredEvents(scope *Scope) map[string]struct{} {
	desired := make(map[string]struct{})

	for _, sub := range r.subscribersFor(scope) {
		for _, e := range sub.DesiredEvents() {
			desired[e] = struct{}{}
		}
	}

	return desired
}

func unionEvents(m map[string]struct{}) []string {
	if _, ok := m["*"]; ok {
		return []string{"*"}
	}

	return slices.Sorted(maps.Keys(m))
}

func (r *Registry) reconcile(ctx context.Context, scope *Scope) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if scope == nil {
		return nil
	}

	mh, exists := r.hooks[*scope]
	desired := r.desiredEvents(scope)

	if len(desired) == 0 {
		if !exists {
			return nil
		}

		r.log.Info("no subscribers remain, deactivating webhook", "scope", *scope)
		if err := r.teardown(mh); err != nil {
			r.log.Warn("error tearing down webhook", "scope", *scope, "error", err)
		}

		delete(r.hooks, *scope)
		return nil
	}

	if !exists {
		hook, err := r.createHook(*scope, unionEvents(desired))
		if err != nil {
			return err
		}

		mh = &managedHook{
			scope:   *scope,
			hook:    hook,
			log:     r.log.With("scope", *scope),
			desired: desired,
			owned:   true,
		}
		r.hooks[*scope] = mh

		return mh.start(ctx, r)
	}

	mh.mu.Lock()
	defer mh.mu.Unlock()

	current := eventSet(mh.hook.Events)
	if maps.Equal(current, desired) && mh.activated {
		return nil
	}

	r.log.Info("updating webhook state", "scope", *scope, "from", mh.hook.Events, "to", unionEvents(desired), "restart", !mh.activated)

	updated, err := r.gh.UpdateHook(*scope, mh.hook.ID, unionEvents(desired), true)
	if err != nil {
		return fmt.Errorf("updating hook: %w", err)
	}

	wasActivated := mh.activated
	mh.hook = updated
	mh.desired = desired
	if !wasActivated {
		return mh.start(ctx, r)
	}
	mh.activated = true

	return nil
}

func (r *Registry) createHook(scope Scope, events []string) (Hook, error) {
	hook, err := r.gh.CreateHook(scope, events, r.secret)
	if err != nil {
		return Hook{}, err
	}

	hook, err = r.gh.Activate(scope, hook.ID)
	if err != nil {
		_ = r.gh.DeleteHook(scope, hook.ID)
		return Hook{}, err
	}

	hook.Active = true
	r.log.Info("created dev webhook", "scope", scope, "id", hook.ID)

	return hook, nil
}

func isNotFound(err error) bool {
	var apiErr *api.HTTPError
	return errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusNotFound
}

func (r *Registry) teardown(mh *managedHook) error {
	if mh.cancel != nil {
		mh.cancel()
	}

	if !mh.owned {
		return nil
	}

	if err := r.gh.DeleteHook(mh.scope, mh.hook.ID); err != nil {
		if isNotFound(err) {
			r.log.Debug("webhook already removed by github", "scope", mh.scope, "id", mh.hook.ID)
			return nil
		}
		return err
	}

	return nil
}

func (r *Registry) Dispatch(ev Event) {
	for _, sub := range r.subscribersFor(&ev.Scope) {
		if err := sub.Deliver(ev); err != nil {
			r.log.Warn("error delivering event to subscriber", "error", err)
		}
	}
}

func (r *Registry) Shutdown() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	var g errgroup.Group
	for _, mh := range r.hooks {
		g.Go(func() error {
			if mh.cancel != nil {
				mh.cancel()
			}

			if !mh.owned {
				return nil
			}

			if err := r.gh.DeleteHook(mh.scope, mh.hook.ID); err != nil {
				if isNotFound(err) {
					return nil
				}
				return err
			}

			return nil
		})
	}

	return g.Wait()
}

func (mh *managedHook) start(ctx context.Context, r *Registry) error {
	ws := NewWebsocketReader(mh.scope, r.gh.Token(), mh.hook.WsURL, mh.log, r.Dispatch)

	if mh.cancel != nil {
		mh.cancel()
	}

	wsCtx, cancel := context.WithCancel(ctx)
	mh.cancel = cancel
	mh.activated = true

	go func() {
		if err := ws.Run(wsCtx); err != nil {
			if wsCtx.Err() != nil {
				return
			}
			mh.log.Warn("websocket reader exited with error", "error", err)
			r.recoverHook(ctx, mh.scope)
		}
	}()

	return nil
}

func (r *Registry) recoverHook(ctx context.Context, scope Scope) {
	if ctx.Err() != nil {
		return
	}

	r.mu.Lock()
	mh, ok := r.hooks[scope]
	if !ok {
		r.mu.Unlock()
		return
	}

	mh.mu.Lock()
	mh.activated = false
	mh.cancel = nil
	mh.mu.Unlock()
	r.mu.Unlock()

	if err := r.reconcile(ctx, &scope); err != nil {
		r.log.Warn("failed to recover webhook", "scope", scope, "error", err)
	}
}

func eventSet(events []string) map[string]struct{} {
	s := make(map[string]struct{}, len(events))
	for _, e := range events {
		s[e] = struct{}{}
	}
	return s
}
