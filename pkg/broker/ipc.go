package broker

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

const maxSubscribeRequestBytes = 64 * 1024

func DefaultSocketPath() (string, error) {
	if dir := os.Getenv("XDG_RUNTIME_DIR"); dir != "" {
		return filepath.Join(dir, "gh-webhook-broker.sock"), nil
	}

	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}

	return filepath.Join(home, ".local", "run", "gh-webhook-broker.sock"), nil
}

type IPCServer struct {
	addr     string
	listener net.Listener
	registry *Registry
	log      *slog.Logger
}

func NewUnixIPCServer(path string, registry *Registry, log *slog.Logger) (*IPCServer, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("creating socket directory: %w", err)
	}

	l, err := listenSocket(path)
	if err != nil {
		return nil, err
	}

	if err := os.Chmod(path, 0o600); err != nil {
		_ = l.Close()
		return nil, fmt.Errorf("chmod socket: %w", err)
	}

	return &IPCServer{
		addr:     "unix://" + path,
		listener: l,
		registry: registry,
		log:      log,
	}, nil
}

func NewTCPIPCServer(addr string, registry *Registry, log *slog.Logger) (*IPCServer, error) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listening on %s: %w", addr, err)
	}

	return &IPCServer{
		addr:     "tcp://" + l.Addr().String(),
		listener: l,
		registry: registry,
		log:      log,
	}, nil
}

func (s *IPCServer) Addr() string { return s.addr }

func (s *IPCServer) Serve(ctx context.Context) error {
	go func() {
		<-ctx.Done()
		s.listener.Close()
	}()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("accept: %w", err)
		}

		go s.handleConn(ctx, conn)
	}
}

func (s *IPCServer) Close() error {
	if strings.HasPrefix(s.addr, "unix://") {
		_ = os.Remove(strings.TrimPrefix(s.addr, "unix://"))
	}
	return s.listener.Close()
}

func listenSocket(path string) (net.Listener, error) {
	l, err := net.Listen("unix", path)
	if err == nil {
		return l, nil
	}

	if !isAddressInUse(err) {
		return nil, fmt.Errorf("listening on %s: %w", path, err)
	}

	conn, dialErr := net.Dial("unix", path)
	if dialErr == nil {
		_ = conn.Close()
		return nil, fmt.Errorf("broker socket %s is already in use", path)
	}

	if !isStaleSocketError(dialErr) {
		return nil, fmt.Errorf("checking existing socket %s: %w", path, dialErr)
	}

	if removeErr := os.Remove(path); removeErr != nil {
		return nil, fmt.Errorf("removing stale socket %s: %w", path, removeErr)
	}

	l, err = net.Listen("unix", path)
	if err != nil {
		return nil, fmt.Errorf("listening on %s: %w", path, err)
	}

	return l, nil
}

func isAddressInUse(err error) bool {
	var opErr *net.OpError
	return errors.As(err, &opErr) && errors.Is(opErr.Err, syscall.EADDRINUSE)
}

func isStaleSocketError(err error) bool {
	return errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ENOENT)
}

func (s *IPCServer) handleConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()

	br := bufio.NewReader(conn)

	msg, err := s.readSubscribeRequest(br)
	if err != nil {
		s.writeError(conn, err.Error())
		return
	}

	sub, err := NewSubscriber(*msg, conn, s.log.With("subscriber", conn.RemoteAddr()))
	if err != nil {
		s.writeError(conn, "error creating subscriber: "+err.Error())
		return
	}

	if err := s.registry.AddSubscriber(ctx, sub); err != nil {
		s.writeError(conn, "error subscribing: "+err.Error())
		return
	}

	defer func() {
		_ = s.registry.RemoveSubscriber(context.Background(), sub)
	}()

	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()

	_, _ = io.Copy(io.Discard, conn)
}

func (s *IPCServer) readSubscribeRequest(br *bufio.Reader) (*SubscribeRequest, error) {
	line, err := readLineLimited(br, maxSubscribeRequestBytes)
	if err != nil {
		return nil, fmt.Errorf("error reading subscribe request: %w", err)
	}

	var msg Message
	if err := json.Unmarshal([]byte(strings.TrimRight(line, "\n")), &msg); err != nil {
		return nil, fmt.Errorf("error parsing subscribe request: %w", err)
	}

	if msg.Subscribe == nil {
		return nil, fmt.Errorf("expected subscribe message")
	}

	return msg.Subscribe, nil
}

func readLineLimited(br *bufio.Reader, limit int) (string, error) {
	var b strings.Builder
	for b.Len() <= limit {
		c, err := br.ReadByte()
		if err != nil {
			return "", err
		}
		b.WriteByte(c)
		if c == '\n' {
			return b.String(), nil
		}
	}
	return "", fmt.Errorf("line exceeds %d bytes", limit)
}

func (s *IPCServer) writeError(conn net.Conn, msg string) {
	m := Message{Error: msg}
	data, _ := json.Marshal(&m)
	data = append(data, '\n')

	_, _ = conn.Write(data)
}
