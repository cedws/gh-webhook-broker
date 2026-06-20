package broker

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
)

type Client struct {
	conn net.Conn
	br   *bufio.Reader
}

type readResult struct {
	ev  *Event
	err error
}

func DialClient(socketPath string) (*Client, error) {
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		return nil, fmt.Errorf("dialing broker socket %s: %w", socketPath, err)
	}

	return &Client{
		conn: conn,
		br:   bufio.NewReader(conn),
	}, nil
}

func (c *Client) Subscribe(req SubscribeRequest) error {
	msg := Message{Subscribe: &req}

	data, err := json.Marshal(&msg)
	if err != nil {
		return err
	}
	data = append(data, '\n')

	_, err = c.conn.Write(data)
	return err
}

func (c *Client) NextEvent(ctx context.Context) (*Event, error) {
	ch := make(chan readResult, 1)
	go func() {
		ch <- c.readEvent()
	}()

	select {
	case <-ctx.Done():
		_ = c.conn.Close()
		return nil, ctx.Err()
	case r := <-ch:
		return r.ev, r.err
	}
}

func (c *Client) readEvent() readResult {
	line, err := c.br.ReadString('\n')
	if err != nil {
		return readResult{nil, err}
	}

	var msg Message
	if err := json.Unmarshal([]byte(strings.TrimRight(line, "\n")), &msg); err != nil {
		return readResult{nil, fmt.Errorf("parsing message: %w", err)}
	}

	if msg.Error != "" {
		return readResult{nil, fmt.Errorf("%s", msg.Error)}
	}

	return readResult{msg.Event, nil}
}

func (c *Client) Close() error {
	return c.conn.Close()
}
