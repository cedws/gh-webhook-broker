package broker

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/cli/go-gh/v2/pkg/auth"
)

type Config struct {
	GitHubHost string
	Addrs      []string
	Secret     string
	Debug      bool
}

func tokenForHost(host string) (string, error) {
	token, _ := auth.TokenForHost(host)
	if token == "" {
		return "", fmt.Errorf("no gh auth token found for host %q (run `gh auth login`)", host)
	}
	return token, nil
}

func Run(ctx context.Context, config Config) error {
	log := setupLogger(config.Debug)

	if config.GitHubHost == "" {
		config.GitHubHost = "github.com"
	}

	token, err := tokenForHost(config.GitHubHost)
	if err != nil {
		return err
	}

	if len(config.Addrs) == 0 {
		p, err := DefaultSocketPath()
		if err != nil {
			return err
		}
		config.Addrs = []string{p}
	}

	gh, err := NewGitHubClient(config.GitHubHost, token)
	if err != nil {
		return err
	}

	registry := NewRegistry(gh, config.Secret, log)

	servers, err := startServers(config.Addrs, registry, log)
	if err != nil {
		return err
	}
	defer func() {
		for _, s := range servers {
			_ = s.Close()
		}
	}()

	log.Info("broker listening",
		"github_host", config.GitHubHost,
		"addrs", serverAddrs(servers),
	)

	errCh := make(chan error, len(servers))
	for _, s := range servers {
		go func(s *IPCServer) { errCh <- s.Serve(ctx) }(s)
	}

	select {
	case <-ctx.Done():
		log.Info("shutting down")
		return registry.Shutdown()
	case err := <-errCh:
		return err
	}
}

func startServers(addrs []string, registry *Registry, log *slog.Logger) ([]*IPCServer, error) {
	var servers []*IPCServer

	for _, addr := range addrs {
		s, err := newServer(addr, registry, log)
		if err != nil {
			return nil, fmt.Errorf("listener %s: %w", addr, err)
		}
		servers = append(servers, s)
	}

	if len(servers) == 0 {
		return nil, fmt.Errorf("no listeners configured")
	}

	return servers, nil
}

func newServer(addr string, registry *Registry, log *slog.Logger) (*IPCServer, error) {
	network, target := parseAddr(addr)

	switch network {
	case "tcp":
		return NewTCPIPCServer(target, registry, log)
	case "unix":
		return NewUnixIPCServer(target, registry, log)
	default:
		return nil, fmt.Errorf("unsupported scheme %q", network)
	}
}

func serverAddrs(servers []*IPCServer) []string {
	addrs := make([]string, len(servers))
	for i, s := range servers {
		addrs[i] = s.Addr()
	}
	return addrs
}

func setupLogger(debug bool) *slog.Logger {
	if debug {
		logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		}))
		slog.SetDefault(logger)
		return logger
	}
	return slog.Default()
}
