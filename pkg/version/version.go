package version

import "fmt"

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func String() string {
	return fmt.Sprintf("gh-webhook-broker %s (commit: %s, built: %s)", version, commit, date)
}
