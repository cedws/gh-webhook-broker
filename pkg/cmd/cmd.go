package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/alecthomas/kong"
	"github.com/cedws/gh-webhook-broker/pkg/broker"
	"github.com/cedws/gh-webhook-broker/pkg/version"
)

type Mode string

const (
	ModeDaemon    Mode = "daemon"
	ModeWait      Mode = "wait"
	ModeSubscribe Mode = "subscribe"
)

type cli struct {
	Debug bool `help:"Enable debug logging."`

	Daemon    daemonCmd    `cmd:"" name:"daemon" help:"Run the broker daemon."`
	Wait      waitCmd      `cmd:"" name:"wait" help:"Connect to the broker and exit on first matching event."`
	Subscribe subscribeCmd `cmd:"" name:"subscribe" help:"Connect to the broker and stream matching events to stdout."`
	Version   versionCmd   `cmd:"" name:"version" help:"Print the broker version."`
}

type daemonCmd struct {
	GitHubHost string   `name:"github-host" short:"H" default:"github.com" env:"GH_HOST" help:"GitHub host name."`
	Socket     []string `name:"socket" short:"s" help:"Listen address (repeatable). Accepts unix:///path/to/sock or tcp://host:port. Defaults to the Unix socket at $XDG_RUNTIME_DIR/gh-webhook-broker.sock."`
	Secret     string   `name:"secret" short:"S" env:"GH_WEBHOOK_SECRET" help:"Webhook secret for incoming events."`
}

type waitCmd struct {
	Events []string `name:"event" short:"E" required:"" help:"GitHub event types to wait for (repeatable). Use '*' for all events."`
	Repo   []string `name:"repo" short:"R" help:"Restrict to a repo (owner/repo). Repeatable."`
	Org    []string `name:"org" short:"O" help:"Restrict to an org. Repeatable."`
	Match  string   `name:"match" short:"M" help:"CEL expression to filter events."`
	Addr   string   `name:"addr" short:"a" help:"Broker address. Unix socket path (default) or tcp://host:port."`
}

type subscribeCmd struct {
	Events []string `name:"event" short:"E" required:"" help:"GitHub event types to subscribe to (repeatable). Use '*' for all events."`
	Repo   []string `name:"repo" short:"R" help:"Restrict to a repo (owner/repo). Repeatable."`
	Org    []string `name:"org" short:"O" help:"Restrict to an org. Repeatable."`
	Match  string   `name:"match" short:"M" help:"CEL expression to filter events."`
	Addr   string   `name:"addr" short:"a" help:"Broker address. Unix socket path (default) or tcp://host:port."`
}

type versionCmd struct{}

func (c *daemonCmd) Run(global *cli) error {
	ctx, stop := signalCtx()
	defer stop()

	cfg := broker.Config{
		GitHubHost: c.GitHubHost,
		Addrs:      c.Socket,
		Secret:     c.Secret,
		Debug:      global.Debug,
	}
	return broker.Run(ctx, cfg)
}

func (c *waitCmd) Run(global *cli) error {
	return runClient(c.Addr, c.Repo, c.Org, c.Events, c.Match, clientModeWait)
}

func (c *subscribeCmd) Run(global *cli) error {
	return runClient(c.Addr, c.Repo, c.Org, c.Events, c.Match, clientModeSubscribe)
}

func (c *versionCmd) Run(global *cli) error {
	fmt.Printf("gh-webhook-broker %s (commit: %s, built: %s)\n", version.Version(), version.Commit(), version.Date())
	return nil
}

type clientMode int

const (
	clientModeWait clientMode = iota
	clientModeSubscribe
)

func runClient(addr string, repos, orgs []string, events []string, match string, mode clientMode) error {
	addr, err := resolveAddr(addr)
	if err != nil {
		return err
	}

	req, err := buildSubscribeRequest(events, match, repos, orgs)
	if err != nil {
		return err
	}

	client, err := broker.DialClient(addr)
	if err != nil {
		return err
	}
	defer client.Close()

	if err := client.Subscribe(req); err != nil {
		return err
	}

	return streamEvents(client, mode)
}

func resolveAddr(addr string) (string, error) {
	if addr != "" {
		return addr, nil
	}

	p, err := broker.DefaultSocketPath()
	if err != nil {
		return "", err
	}
	return p, nil
}

func buildSubscribeRequest(events []string, match string, repos, orgs []string) (broker.SubscribeRequest, error) {
	if len(repos) == 0 && len(orgs) == 0 {
		return broker.SubscribeRequest{}, fmt.Errorf("at least one --repo or --org is required")
	}

	req := broker.SubscribeRequest{Events: events, Match: match}

	for _, repo := range repos {
		owner, name, found := strings.Cut(repo, "/")
		if !found || owner == "" || name == "" || strings.Contains(name, "/") {
			return broker.SubscribeRequest{}, fmt.Errorf("invalid repo %q, expected owner/repo", repo)
		}
		req.Scopes = append(req.Scopes, broker.RepoScope(owner, name))
	}

	for _, org := range orgs {
		if org == "" || strings.Contains(org, "/") {
			return broker.SubscribeRequest{}, fmt.Errorf("invalid org %q, expected org name", org)
		}
		req.Scopes = append(req.Scopes, broker.OrgScope(org))
	}

	return req, nil
}

func streamEvents(client *broker.Client, mode clientMode) error {
	ctx, stop := signalCtx()
	defer stop()

	for {
		ev, err := client.NextEvent(ctx)
		if err != nil {
			return err
		}

		if mode == clientModeWait {
			return nil
		}

		data, err := json.Marshal(ev)
		if err != nil {
			return fmt.Errorf("marshalling event: %w", err)
		}

		if _, err := os.Stdout.Write(data); err != nil {
			return err
		}

		if _, err := os.Stdout.Write([]byte("\n")); err != nil {
			return err
		}
	}
}

func Execute(mode Mode) {
	var c cli

	args := os.Args[1:]
	if mode == ModeWait || mode == ModeSubscribe {
		args = append([]string{string(mode)}, args...)
	}

	parser := kong.Must(&c,
		kong.Name("gh-webhook-broker"),
		kong.Description("Broker for GitHub dev webhooks."),
		kong.UsageOnError(),
	)

	kctx, err := parser.Parse(args)
	if err != nil {
		parser.Fatalf("%s", err)
	}

	kctx.FatalIfErrorf(kctx.Run(&c))
}

func signalCtx() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}
