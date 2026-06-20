package main

import (
	"os"
	"path/filepath"

	"github.com/cedws/gh-webhook-broker/pkg/cmd"
)

func main() {
	mode := cmd.ModeDaemon
	switch filepath.Base(os.Args[0]) {
	case "gh-webhook-wait":
		mode = cmd.ModeWait
	case "gh-webhook-subscribe":
		mode = cmd.ModeSubscribe
	}
	cmd.Execute(mode)
}
