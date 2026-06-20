package broker

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/cel-go/cel"
)

type filter struct {
	prog cel.Program
}

func compileFilter(expr string) (*filter, error) {
	if expr == "" {
		return nil, nil
	}
	env, err := cel.NewEnv(
		cel.Variable("type", cel.StringType),
		cel.Variable("scope", cel.StringType),
		cel.Variable("repo", cel.StringType),
		cel.Variable("event", cel.DynType),
	)
	if err != nil {
		return nil, fmt.Errorf("creating CEL environment: %w", err)
	}
	ast, iss := env.Compile(expr)
	if iss.Err() != nil {
		return nil, fmt.Errorf("compiling CEL expression: %w", iss.Err())
	}
	prog, err := env.Program(ast)
	if err != nil {
		return nil, fmt.Errorf("building CEL program: %w", err)
	}
	return &filter{prog: prog}, nil
}

func (f *filter) eval(ev Event) (bool, error) {
	if f == nil {
		return true, nil
	}

	payload, err := decodePayload(ev.Payload)
	if err != nil {
		return false, err
	}

	vars := map[string]any{
		"type":  ev.Type,
		"scope": ev.Scope.String(),
		"repo":  ev.Scope.Name,
		"event": payload,
	}

	out, _, err := f.prog.Eval(vars)
	if err != nil {
		if isMissingKeyErr(err) {
			return false, nil
		}
		return false, fmt.Errorf("evaluating CEL expression: %w", err)
	}

	b, ok := out.Value().(bool)
	if !ok {
		return false, fmt.Errorf("CEL expression did not return a boolean, got %s", out.Type())
	}

	return b, nil
}

func decodePayload(payload json.RawMessage) (any, error) {
	if len(payload) == 0 {
		return map[string]any{}, nil
	}
	var v any
	if err := json.Unmarshal(payload, &v); err != nil {
		return nil, fmt.Errorf("decoding payload for CEL: %w", err)
	}
	return v, nil
}

func isMissingKeyErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "no such key")
}
