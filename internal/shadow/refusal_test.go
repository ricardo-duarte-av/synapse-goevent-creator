package shadow

import (
	"context"
	"net/http"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/route"
)

type fixedPredictor struct {
	status int
	body   string
	kind   string
}

func (f fixedPredictor) Predict(*http.Request, route.Request) (int, []byte, string, string) {
	return f.status, []byte(f.body), f.kind, "reason"
}

func TestCheckRefusal(t *testing.T) {
	synapse := `{"error":"'body' not in content","errcode":"M_UNKNOWN"}`
	for name, tc := range map[string]struct {
		status int
		pred   fixedPredictor
		want   string
	}{
		"same":        {400, fixedPredictor{400, `{"errcode":"M_UNKNOWN","error":"'body' not in content"}`, predRefused}, Match},
		"other words": {400, fixedPredictor{400, `{"errcode":"M_UNKNOWN","error":"something else"}`, predRefused}, Mismatch},
		"other code":  {400, fixedPredictor{403, `{"errcode":"M_FORBIDDEN","error":"'body' not in content"}`, predRefused}, Mismatch},
		"accepted":    {400, fixedPredictor{200, `{"event_id":"$x"}`, predAccepted}, "accepted"},
		"fallback":    {400, fixedPredictor{0, ``, predFallback}, "skip_fallback"},
		"ratelimit":   {429, fixedPredictor{400, ``, predRefused}, "skip_ratelimit"},
	} {
		c := New(Options{Predictor: tc.pred, Log: zerolog.Nop()})
		before := testutil.ToFloat64(checks.WithLabelValues("send", "refusal", tc.want))
		p := &Pending{req: route.Request{Endpoint: route.Send, Room: "!r:x"}, method: http.MethodPut, uri: "/x", header: http.Header{}}
		c.checkRefusal(context.Background(), p, tc.status, []byte(synapse))
		if testutil.ToFloat64(checks.WithLabelValues("send", "refusal", tc.want)) != before+1 {
			t.Errorf("%s: not counted as %s", name, tc.want)
		}
	}
}
