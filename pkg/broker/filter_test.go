package broker

import (
	"testing"

	"github.com/alecthomas/assert/v2"
)

func TestCompileFilter_Empty(t *testing.T) {
	f, err := CompileFilter("")
	assert.NoError(t, err)
	assert.True(t, f == nil)
}

func TestFilter_Eval_NoFilter(t *testing.T) {
	var f *Filter
	ok, err := f.Eval(Event{Type: "push"})
	assert.NoError(t, err)
	assert.True(t, ok)
}

func TestFilter_Eval_ActionEquality(t *testing.T) {
	f, err := CompileFilter(`event.action == "opened"`)
	assert.NoError(t, err)

	cases := []struct {
		name string
		ev   Event
		want bool
	}{
		{"matches", Event{Type: "issues", Payload: []byte(`{"action":"opened"}`)}, true},
		{"no match", Event{Type: "issues", Payload: []byte(`{"action":"closed"}`)}, false},
		{"missing field", Event{Type: "issues", Payload: []byte(`{}`)}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := f.Eval(tc.ev)
			assert.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestFilter_Eval_TypeAndNested(t *testing.T) {
	f, err := CompileFilter(`type == "pull_request" && event.action == "opened" && !event.pull_request.draft`)
	assert.NoError(t, err)

	cases := []struct {
		name string
		ev   Event
		want bool
	}{
		{
			"opened non-draft",
			Event{Type: "pull_request", Payload: []byte(`{"action":"opened","pull_request":{"draft":false}}`)},
			true,
		},
		{
			"opened draft",
			Event{Type: "pull_request", Payload: []byte(`{"action":"opened","pull_request":{"draft":true}}`)},
			false,
		},
		{
			"wrong type",
			Event{Type: "issues", Payload: []byte(`{"action":"opened"}`)},
			false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := f.Eval(tc.ev)
			assert.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestFilter_Eval_Scope(t *testing.T) {
	f, err := CompileFilter(`repo == "cedws/microrunner"`)
	assert.NoError(t, err)

	ev := Event{Type: "push", Scope: RepoScope("cedws", "microrunner")}
	got, err := f.Eval(ev)
	assert.NoError(t, err)
	assert.True(t, got)
}

func TestFilter_Eval_OrgScope(t *testing.T) {
	f, err := CompileFilter(`scope == "orgs/acme"`)
	assert.NoError(t, err)

	ev := Event{Type: "push", Scope: OrgScope("acme")}
	got, err := f.Eval(ev)
	assert.NoError(t, err)
	assert.True(t, got)
}

func TestCompileFilter_Invalid(t *testing.T) {
	_, err := CompileFilter(`event.action ==`)
	assert.Error(t, err)
}

func TestCompileFilter_NonBool(t *testing.T) {
	f, err := CompileFilter(`event.action`)
	assert.NoError(t, err)

	_, err = f.Eval(Event{Type: "issues", Payload: []byte(`{"action":"opened"}`)})
	assert.Error(t, err)
}
