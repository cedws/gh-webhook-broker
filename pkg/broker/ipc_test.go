package broker

import (
	"bufio"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/alecthomas/assert/v2"
)

func TestNewIPCServer_RejectsActiveSocket(t *testing.T) {
	path := testSocketPath(t)

	server, err := NewUnixIPCServer(path, &Registry{}, testLogger())
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, server.Close())
	})

	other, err := NewUnixIPCServer(path, &Registry{}, testLogger())
	assert.Error(t, err)
	assert.True(t, other == nil)
}

func TestNewIPCServer_ReusesStaleSocket(t *testing.T) {
	path := testSocketPath(t)

	listener, err := net.Listen("unix", path)
	assert.NoError(t, err)
	assert.NoError(t, listener.Close())

	server, err := NewUnixIPCServer(path, &Registry{}, testLogger())
	assert.NoError(t, err)
	assert.NoError(t, server.Close())
}

func TestReadSubscribeRequest_RejectsOversizedLine(t *testing.T) {
	server := &IPCServer{}
	body := strings.Repeat("a", maxSubscribeRequestBytes+1) + "\n"

	_, err := server.readSubscribeRequest(bufio.NewReader(strings.NewReader(body)))
	assert.Error(t, err)
}

func testSocketPath(t *testing.T) string {
	t.Helper()

	dir, err := os.MkdirTemp("/tmp", "ghwb-")
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, os.RemoveAll(dir))
	})

	return filepath.Join(dir, "broker.sock")
}
