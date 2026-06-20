package broker

import (
	"encoding/json"
	"fmt"
)

type Scope struct {
	Kind Kind   `json:"kind"`
	Name string `json:"name"`
}

type Kind string

const (
	KindRepo Kind = "repo"
	KindOrg  Kind = "org"
)

func (s Scope) String() string {
	if s.Kind == KindOrg {
		return fmt.Sprintf("orgs/%s", s.Name)
	}
	return s.Name
}

func RepoScope(owner, repo string) Scope {
	return Scope{Kind: KindRepo, Name: owner + "/" + repo}
}

func OrgScope(org string) Scope {
	return Scope{Kind: KindOrg, Name: org}
}

type Event struct {
	Type    string          `json:"type"`
	Scope   Scope           `json:"scope"`
	Payload json.RawMessage `json:"payload"`
}

type SubscribeRequest struct {
	Events []string `json:"events,omitempty"`
	Scopes []Scope  `json:"scopes,omitempty"`
	Match  string   `json:"match,omitempty"`
}

type Message struct {
	Subscribe *SubscribeRequest `json:"subscribe,omitempty"`
	Event     *Event            `json:"event,omitempty"`
	Error     string            `json:"error,omitempty"`
}
