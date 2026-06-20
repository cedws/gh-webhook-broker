package cmd

import (
	"testing"

	"github.com/alecthomas/assert/v2"
	"go.cedwards.xyz/gh-webhook-broker/pkg/broker"
)

func TestBuildSubscribeRequest_MultipleReposAndOrgs(t *testing.T) {
	req, err := buildSubscribeRequest([]string{"push"}, "", []string{"cedws/test", "cedws/iapc"}, []string{"acme"})
	assert.NoError(t, err)

	assert.Equal(t, 3, len(req.Scopes))
	assert.Equal(t, broker.RepoScope("cedws", "test"), req.Scopes[0])
	assert.Equal(t, broker.RepoScope("cedws", "iapc"), req.Scopes[1])
	assert.Equal(t, broker.OrgScope("acme"), req.Scopes[2])
}

func TestBuildSubscribeRequest_RejectsMalformedRepo(t *testing.T) {
	_, err := buildSubscribeRequest([]string{"push"}, "", []string{"cedws/gh/webhook-broker"}, nil)
	assert.Error(t, err)
}

func TestBuildSubscribeRequest_RejectsNoScopes(t *testing.T) {
	_, err := buildSubscribeRequest([]string{"push"}, "", nil, nil)
	assert.Error(t, err)
}

func TestBuildSubscribeRequest_RejectsMalformedOrg(t *testing.T) {
	_, err := buildSubscribeRequest([]string{"push"}, "", nil, []string{"acme/platform"})
	assert.Error(t, err)
}
