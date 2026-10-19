package userips

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// The line is Synapse's: checked against UserIpCommand.to_line in its
// container.
func TestLine(t *testing.T) {
	dev := "DEV"
	got := Line("@u:example.org", "syt_tok", "1.2.3.4", "Element/1.0 (é)", &dev, 1700000000000)
	want := "@u:example.org [\"syt_tok\",\"1.2.3.4\",\"Element/1.0 (\\u00e9)\",\"DEV\",1700000000000]"
	if got != want {
		t.Errorf("%s\nwant %s", got, want)
	}
	if got := Line("@u:example.org", "t", "::1", "", nil, 1); got != `@u:example.org ["t","::1","",null,1]` {
		t.Errorf("%s", got)
	}
}

type pubs struct {
	mu    sync.Mutex
	lines []string
	done  chan struct{}
}

func (p *pubs) Publish(_ context.Context, channel, line string) error {
	p.mu.Lock()
	p.lines = append(p.lines, channel+" "+line)
	p.mu.Unlock()
	p.done <- struct{}{}
	return nil
}

func TestRecordOncePerTwoMinutes(t *testing.T) {
	now := time.UnixMilli(1700000000000)
	p := &pubs{done: make(chan struct{}, 10)}
	r := New(p, "example.org", zerolog.Nop(), func() time.Time { return now })
	r.Record("@u:example.org", "t", "1.2.3.4", "ua", nil)
	<-p.done
	r.Record("@u:example.org", "t", "1.2.3.4", "ua", nil)
	now = now.Add(121 * time.Second)
	r.Record("@u:example.org", "t", "1.2.3.4", "ua", nil)
	<-p.done
	r.Record("@u:example.org", "t", "1.2.3.4", "sync-v3-proxy-", nil)
	if len(p.lines) != 2 || p.lines[0] != `example.org/USER_IP USER_IP @u:example.org ["t","1.2.3.4","ua",null,1700000000000]` {
		t.Errorf("%q", p.lines)
	}
}
