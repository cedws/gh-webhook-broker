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
	SocketPath string
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

	if config.SocketPath == "" {
		p, err := DefaultSocketPath()
		if err != nil {
			return err
		}
		config.SocketPath = p
	}

	gh, err := NewGitHubClient(config.GitHubHost, token)
	if err != nil {
		return err
	}

	registry := NewRegistry(gh, config.Secret, log)

	ipc, err := NewIPCServer(config.SocketPath, registry, log)
	if err != nil {
		return err
	}
	defer ipc.Close()

	log.Info("broker listening", "socket", ipc.Path(), "host", config.GitHubHost)

	errCh := make(chan error, 1)
	go func() { errCh <- ipc.Serve(ctx) }()

	select {
	case <-ctx.Done():
		log.Info("shutting down")
		return registry.Shutdown()
	case err := <-errCh:
		return err
	}
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
