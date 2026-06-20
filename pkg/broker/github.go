package broker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/cli/go-gh/v2/pkg/api"
)

type hookConfig struct {
	ContentType string `json:"content_type"`
	InsecureSSL string `json:"insecure_ssl"`
	URL         string `json:"url"`
	Secret      string `json:"secret,omitempty"`
}

type createHookRequest struct {
	Name   string     `json:"name"`
	Events []string   `json:"events"`
	Active bool       `json:"active"`
	Config hookConfig `json:"config"`
}

type hook struct {
	ID     int        `json:"id"`
	Name   string     `json:"name"`
	Active bool       `json:"active"`
	Events []string   `json:"events"`
	Config hookConfig `json:"config"`
	URL    string     `json:"url"`
	WsURL  string     `json:"ws_url"`
}

type gitHubClient struct {
	host      string
	authToken string
	rest      *api.RESTClient
}

func newGitHubClient(host, token string) (*gitHubClient, error) {
	rest, err := api.NewRESTClient(api.ClientOptions{
		Host:      host,
		AuthToken: token,
	})
	if err != nil {
		return nil, fmt.Errorf("creating REST client: %w", err)
	}

	return &gitHubClient{host: host, authToken: token, rest: rest}, nil
}

func (c *gitHubClient) scopePath(s Scope) string {
	switch s.Kind {
	case KindOrg:
		return fmt.Sprintf("orgs/%s/hooks", s.Name)
	case KindRepo:
		return fmt.Sprintf("repos/%s/hooks", s.Name)
	default:
		return ""
	}
}

func (c *gitHubClient) listHooks(s Scope) ([]hook, error) {
	var hooks []hook
	if err := c.rest.Get(c.scopePath(s), &hooks); err != nil {
		return nil, fmt.Errorf("listing hooks for %s %s: %w", s.Kind, s.Name, err)
	}
	return hooks, nil
}

func (c *gitHubClient) CreateHook(s Scope, events []string, secret string) (hook, error) {
	req := createHookRequest{
		Name:   "cli",
		Events: events,
		Active: false,
		Config: hookConfig{
			ContentType: "json",
			InsecureSSL: "0",
			Secret:      secret,
		},
	}

	body, err := json.Marshal(req)
	if err != nil {
		return hook{}, err
	}

	var h hook
	if err := c.rest.Post(c.scopePath(s), bytes.NewReader(body), &h); err != nil {
		return hook{}, fmt.Errorf("creating dev webhook for %s %s: %w", s.Kind, s.Name, err)
	}

	return h, nil
}

func (c *gitHubClient) UpdateHook(s Scope, id int, events []string, active bool) (hook, error) {
	patch := struct {
		Events []string `json:"events,omitempty"`
		Active *bool    `json:"active,omitempty"`
	}{
		Events: events,
		Active: &active,
	}

	body, err := json.Marshal(patch)
	if err != nil {
		return hook{}, err
	}

	var h hook
	path := fmt.Sprintf("%s/%d", c.scopePath(s), id)
	if err := c.rest.Patch(path, bytes.NewReader(body), &h); err != nil {
		return hook{}, fmt.Errorf("updating dev webhook %d for %s %s: %w", id, s.Kind, s.Name, err)
	}

	return h, nil
}

func (c *gitHubClient) DeleteHook(s Scope, id int) error {
	path := fmt.Sprintf("%s/%d", c.scopePath(s), id)
	if err := c.rest.Delete(path, nil); err != nil {
		return fmt.Errorf("deleting dev webhook %d for %s %s: %w", id, s.Kind, s.Name, err)
	}
	return nil
}

func (c *gitHubClient) Activate(s Scope, id int) (hook, error) {
	path := fmt.Sprintf("%s/%d", c.scopePath(s), id)
	body := strings.NewReader(`{"active":true}`)
	var h hook
	if err := c.rest.Patch(path, body, &h); err != nil {
		return hook{}, fmt.Errorf("activating dev webhook %d for %s %s: %w", id, s.Kind, s.Name, err)
	}
	return h, nil
}

func (c *gitHubClient) token() string { return c.authToken }
