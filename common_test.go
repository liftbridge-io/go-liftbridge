package liftbridge

import (
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/liftbridge-io/liftbridge/server"
)

// Used by both testing.B and testing.T so need to use
// a common interface: tLogger
type tLogger interface {
	Fatalf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

func stackFatalf(t tLogger, f string, args ...interface{}) {
	lines := make([]string, 0, 32)
	msg := fmt.Sprintf(f, args...)
	lines = append(lines, msg)

	// Generate the Stack of callers:
	for i := 1; true; i++ {
		_, file, line, ok := runtime.Caller(i)
		if !ok {
			break
		}
		msg := fmt.Sprintf("%d - %s:%d", i, file, line)
		lines = append(lines, msg)
	}

	t.Fatalf("%s", strings.Join(lines, "\n"))
}

func getMetadataLeader(t *testing.T, timeout time.Duration, servers ...*server.Server) *server.Server {
	var (
		leader   *server.Server
		deadline = time.Now().Add(timeout)
	)
	for time.Now().Before(deadline) {
		for _, s := range servers {
			if !s.IsRunning() {
				continue
			}
			if s.IsLeader() {
				if leader != nil {
					stackFatalf(t, "Found more than one metadata leader")
				}
				leader = s
			}
		}
		if leader != nil {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	if leader == nil {
		stackFatalf(t, "No metadata leader found")
	}
	return leader
}
