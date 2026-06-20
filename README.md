# gh-webhook-broker

A daemon that subscribes to GitHub dev webhooks and brokers events to local subscribers.

## Overview

`gh-webhook-broker` is a multicall binary with three modes:

- **`gh-webhook-broker daemon`** — Runs the broker daemon. Creates GitHub dev webhooks on demand, maintains websocket connections to receive events, and fans them out to connected subscribers.
- **`gh-webhook-wait`** — Connects to the broker, subscribes to specific events, and exits when a matching event arrives.
- **`gh-webhook-subscribe`** — Connects to the broker, subscribes to specific event types, and streams matching events to stdout.

The binary uses argv[0] dispatch: symlink or rename it to `gh-webhook-wait` or `gh-webhook-subscribe` to invoke those modes directly. On systems where symlinks aren't available (e.g. Windows), the same functionality is available via subcommands:

```fish
gh-webhook-broker wait --event=push --repo=cedws/gh-webhook-broker
gh-webhook-broker subscribe --event=push --repo=cedws/gh-webhook-broker
```

## Prerequisites

- GitHub CLI (`gh`) authenticated with `gh auth login`. The token needs `repo` scope (for repo webhooks) and `admin:org_hook` scope (for org webhooks).

## Usage

### Start the broker

```fish
gh-webhook-broker daemon --debug
```

By default the broker listens on a Unix socket at `$XDG_RUNTIME_DIR/gh-webhook-broker.sock`. You can override the listen address with `--socket` (repeatable), accepting `unix:///path/to/sock` or `tcp://host:port`:

```fish
# TCP only
gh-webhook-broker daemon --socket tcp://127.0.0.1:8080

# Both Unix socket and TCP
gh-webhook-broker daemon --socket unix:///tmp/ghwb.sock --socket tcp://127.0.0.1:8080
```

### Wait for a specific event

Block until a pull request is opened on a repo:

```fish
gh-webhook-wait --repo=cedws/iapc --event=pull_request \
  --match 'event.action == "opened" && !event.pull_request.draft'
```

Wait for CI to finish on a pull request (check run completes with a conclusion):

```fish
gh-webhook-wait --repo=cedws/iapc --event=check_run \
  --match 'event.action == "completed" && event.check_run.conclusion == "success"'
```

### Stream events to stdout

Stream all push events from an org:

```fish
gh-webhook-subscribe --org=acme-corp --event=push
```

Connect to a remote broker over TCP:

```fish
gh-webhook-subscribe --addr tcp://127.0.0.1:8080 --repo=cedws/iapc --event=push
```

### Flags

```
gh-webhook-broker daemon [--github-host=github.com] [--socket=ADDR] [--secret=SECRET] [--debug]
gh-webhook-wait      --event=TYPE [--event=...] [--repo=owner/repo] [--org=name] [--match=CEL] [--addr=ADDR]
gh-webhook-subscribe --event=TYPE [--event=...] [--repo=owner/repo] [--org=name] [--match=CEL] [--addr=ADDR]
```

`--socket` (daemon) and `--addr` (wait/subscribe) accept `unix:///path/to/sock`, `tcp://host:port`, or a bare path (treated as Unix). Both `--repo` and `--org` are repeatable to subscribe to multiple scopes at once.

### CEL matching

The `--match` flag takes a [CEL](https://cel.dev) expression evaluated against each event. Available variables:

- `type` — the GitHub event type (e.g. `"push"`, `"pull_request"`)
- `event` — the parsed JSON payload as a map
- `repo` — the scope name (e.g. `"cedws/iapc"`)
- `scope` — the full scope string (e.g. `"cedws/iapc"` or `"orgs/acme"`)

Examples:

```fish
# PR opened, not a draft
--match 'type == "pull_request" && event.action == "opened" && !event.pull_request.draft'

# Push to main
--match 'event.ref == "refs/heads/main"'

# Issue labeled "bug"
--match 'event.action == "labeled" && event.label.name == "bug"'
```

### Output format

`gh-webhook-subscribe` prints one JSON object per line, each containing the event type, scope, and payload:

```json
{"type":"push","scope":{"kind":"repo","name":"cedws/iapc"},"payload":{"ref":"refs/heads/main",...}}
```

`gh-webhook-wait` exits silently on the first match.

## How it works

The broker manages GitHub dev webhooks (the same mechanism used by `gh webhook forward`). Dev webhooks provide a websocket URL alongside the standard webhook; GitHub streams events over the websocket without requiring a publicly reachable server.

When a subscriber connects to the broker:

1. The broker computes the union of event types needed across all active subscribers for that repo/org.
2. If no dev webhook exists, one is created with the desired events and activated.
3. If a dev webhook already exists, it's adopted and patched with the updated event set.
4. Events arriving over the websocket are evaluated against each subscriber's CEL filter and delivered as newline-delimited JSON.

When the last subscriber for a scope disconnects, the broker deletes the dev webhook.
