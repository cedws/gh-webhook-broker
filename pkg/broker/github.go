package broker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/cli/go-gh/v2/pkg/api"
)

type HookConfig struct {
	ContentType string `json:"content_type"`
	InsecureSSL string `json:"insecure_ssl"`
	URL         string `json:"url"`
	Secret      string `json:"secret,omitempty"`
}

type createHookRequest struct {
	Name   string     `json:"name"`
	Events []string   `json:"events"`
	Active bool       `json:"active"`
	Config HookConfig `json:"config"`
}

type Hook struct {
	ID     int        `json:"id"`
	Name   string     `json:"name"`
	Active bool       `json:"active"`
	Events []string   `json:"events"`
	Config HookConfig `json:"config"`
	URL    string     `json:"url"`
	WsURL  string     `json:"ws_url"`
}

type GitHubClient struct {
	host  string
	token string
	rest  *api.RESTClient
}

func NewGitHubClient(host, token string) (*GitHubClient, error) {
	rest, err := api.NewRESTClient(api.ClientOptions{
		Host:      host,
		AuthToken: token,
	})
	if err != nil {
		return nil, fmt.Errorf("creating REST client: %w", err)
	}

	return &GitHubClient{host: host, token: token, rest: rest}, nil
}

func (c *GitHubClient) scopePath(s Scope) string {
	switch s.Kind {
	case KindOrg:
		return fmt.Sprintf("orgs/%s/hooks", s.Name)
	case KindRepo:
		return fmt.Sprintf("repos/%s/hooks", s.Name)
	default:
		return ""
	}
}

func (c *GitHubClient) ListHooks(s Scope) ([]Hook, error) {
	var hooks []Hook
	if err := c.rest.Get(c.scopePath(s), &hooks); err != nil {
		return nil, fmt.Errorf("listing hooks for %s %s: %w", s.Kind, s.Name, err)
	}
	return hooks, nil
}

func (c *GitHubClient) CreateHook(s Scope, events []string, secret string) (Hook, error) {
	req := createHookRequest{
		Name:   "cli",
		Events: events,
		Active: false,
		Config: HookConfig{
			ContentType: "json",
			InsecureSSL: "0",
			Secret:      secret,
		},
	}

	body, err := json.Marshal(req)
	if err != nil {
		return Hook{}, err
	}

	var hook Hook
	if err := c.rest.Post(c.scopePath(s), bytes.NewReader(body), &hook); err != nil {
		return Hook{}, fmt.Errorf("creating dev webhook for %s %s: %w", s.Kind, s.Name, err)
	}

	return hook, nil
}

func (c *GitHubClient) UpdateHook(s Scope, id int, events []string, active bool) (Hook, error) {
	patch := struct {
		Events []string `json:"events,omitempty"`
		Active *bool    `json:"active,omitempty"`
	}{
		Events: events,
		Active: &active,
	}

	body, err := json.Marshal(patch)
	if err != nil {
		return Hook{}, err
	}

	var hook Hook
	path := fmt.Sprintf("%s/%d", c.scopePath(s), id)
	if err := c.rest.Patch(path, bytes.NewReader(body), &hook); err != nil {
		return Hook{}, fmt.Errorf("updating dev webhook %d for %s %s: %w", id, s.Kind, s.Name, err)
	}

	return hook, nil
}

func (c *GitHubClient) DeleteHook(s Scope, id int) error {
	path := fmt.Sprintf("%s/%d", c.scopePath(s), id)
	if err := c.rest.Delete(path, nil); err != nil {
		return fmt.Errorf("deleting dev webhook %d for %s %s: %w", id, s.Kind, s.Name, err)
	}
	return nil
}

func (c *GitHubClient) Activate(s Scope, id int) (Hook, error) {
	path := fmt.Sprintf("%s/%d", c.scopePath(s), id)
	body := strings.NewReader(`{"active":true}`)
	var hook Hook
	if err := c.rest.Patch(path, body, &hook); err != nil {
		return Hook{}, fmt.Errorf("activating dev webhook %d for %s %s: %w", id, s.Kind, s.Name, err)
	}
	return hook, nil
}

func (c *GitHubClient) Host() string  { return c.host }
func (c *GitHubClient) Token() string { return c.token }
