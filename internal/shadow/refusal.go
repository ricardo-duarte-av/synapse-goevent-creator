package shadow

import (
	"bytes"
	"context"
	"net/http"

	"github.com/rs/zerolog"
	"github.com/tidwall/gjson"
	"maunium.net/go/mautrix/crypto/canonicaljson"

	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/route"
)

// Predictor is the native path run dry (creator.NewPredictor).
type Predictor interface {
	Predict(r *http.Request, req route.Request) (status int, body []byte, kind string)
}

// The predictor's kinds (creator.Predict*).
const (
	predRefused     = "refused"
	predAccepted    = "accepted"
	predFallback    = "fallback"
	predRatelimited = "ratelimited"
)

// async runs f in the background within the check limit, or drops it.
func (c *Checker) async(ep string, f func(ctx context.Context)) {
	select {
	case c.sem <- struct{}{}:
	default:
		checks.WithLabelValues(ep, "request", "dropped").Inc()
		return
	}
	go func() {
		defer func() { <-c.sem }()
		ctx, cancel := context.WithTimeout(context.Background(), c.o.Timeout)
		defer cancel()
		f(ctx)
	}()
}

// checkRefusal compares Synapse's refusal with the native path's answer to
// the same request, run dry after it: nothing was written for a refusal, so
// the room is as it was. Check "refusal": match (the same status and body),
// mismatch, accepted (we would have served what Synapse refused), or a skip
// for what is not ours to predict: a request the native path leaves to
// Synapse, and ratelimits, which are each process's own.
func (c *Checker) checkRefusal(ctx context.Context, p *Pending, status int, body []byte) {
	ep := string(p.req.Endpoint)
	log := c.o.Log.With().Str("endpoint", ep).Str("room_id", p.req.Room).Logger()
	record := func(result string, detail func(*zerolog.Event)) {
		checks.WithLabelValues(ep, "refusal", result).Inc()
		if result == Mismatch || result == "accepted" {
			e := log.Warn().Str("check", "refusal").Str("result", result).Int("synapse_status", status).
				Str("synapse_errcode", gjson.GetBytes(body, "errcode").Str)
			if detail != nil {
				detail(e)
			}
			e.Msg("Shadow check disagrees with Synapse")
		}
	}
	if status == http.StatusTooManyRequests {
		record("skip_ratelimit", nil)
		return
	}
	r, err := http.NewRequestWithContext(ctx, p.method, p.uri, bytes.NewReader(p.raw))
	if err != nil {
		record(Error, nil)
		return
	}
	r.Header = p.header.Clone()
	ourStatus, ourBody, kind := c.o.Predictor.Predict(r, p.req)
	switch kind {
	case predFallback:
		record("skip_fallback", nil)
		return
	case predRatelimited:
		record("skip_ratelimit", nil)
		return
	case predAccepted:
		record("accepted", func(e *zerolog.Event) {
			e.Int("our_status", ourStatus)
			if !p.private {
				e.RawJSON("synapse", body)
			}
		})
		return
	}
	a, errA := canonicaljson.CanonicalJSON(body)
	b, errB := canonicaljson.CanonicalJSON(ourBody)
	if ourStatus == status && errA == nil && errB == nil && bytes.Equal(a, b) {
		record(Match, nil)
		return
	}
	record(Mismatch, func(e *zerolog.Event) {
		e.Int("our_status", ourStatus).Str("our_errcode", gjson.GetBytes(ourBody, "errcode").Str).
			Bool("same_words", gjson.GetBytes(ourBody, "error").Str == gjson.GetBytes(body, "error").Str)
		if !p.private {
			// Messages can quote content (a jsonschema repr): only in the
			// test rooms.
			e.RawJSON("synapse", body).RawJSON("ours", ourBody)
		}
	})
}
