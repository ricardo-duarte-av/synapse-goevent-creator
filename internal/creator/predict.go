package creator

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/route"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/writer"
)

// A predictor is a Creator that only answers: it runs the native path on a
// request Synapse has answered, to say what the native path would have
// answered (shadow mode's refusal check). It cannot change anything: it is
// built with no writer (writes fail), no events writer, no main, no policy
// server, no remote server and no USER_IP, and it stops before persisting
// and before a delayed event's insert. Its ratelimiters are its own, so a
// ratelimit it predicts says nothing.

// Prediction kinds.
const (
	PredictRefused     = "refused"
	PredictAccepted    = "accepted"
	PredictFallback    = "fallback"
	PredictRatelimited = "ratelimited"
)

// NewPredictor builds a predictor from the native path's options.
func NewPredictor(o Options) *Creator {
	o.Writer = dryWriter{}
	o.Persister = nil
	o.Main = nil
	o.Policy = nil
	o.RemoteProfiles = nil
	o.UserIPs = nil
	o.ShadowPause = func(time.Duration) time.Duration { return 0 }
	c := New(o)
	c.dry = true
	c.verify = func(pending) {}
	return c
}

// Predictor is a predictor with this Creator's options.
func (c *Creator) Predictor() *Creator { return NewPredictor(c.o) }

// Predict answers r as the native path would, without effect: the status
// and body it would write, what kind of answer that is, and for a fallback
// its reason.
func (c *Creator) Predict(r *http.Request, req route.Request) (status int, body []byte, kind, reason string) {
	rec := httptest.NewRecorder()
	var out Outcome
	if req.Endpoint == route.Membership || req.Endpoint == route.Join || req.Endpoint == route.Knock ||
		(req.Endpoint == route.State && req.Action == "m.room.member" && r.Method == http.MethodPut) {
		out = c.serveMembership(rec, r, req)
	} else {
		out = c.serve(rec, r, req)
	}
	switch {
	case out.Fallback != "":
		return 0, nil, PredictFallback, out.Fallback
	case out.Result == "ratelimited":
		return rec.Code, rec.Body.Bytes(), PredictRatelimited, ""
	case out.Refused != "":
		return rec.Code, rec.Body.Bytes(), PredictRefused, out.Refused
	}
	return rec.Code, rec.Body.Bytes(), PredictAccepted, out.Result
}

var errDry = errors.New("creator: a predictor does not write")

// dryWriter refuses every write. A token's first use is a no-op rather than
// a failure, so the prediction goes on as the real request did.
type dryWriter struct{}

func (dryWriter) MarkTokenUsed(context.Context, int64) error { return nil }
func (dryWriter) BumpDeletion(context.Context, []int64) ([]int64, error) {
	return nil, errDry
}
func (dryWriter) SetRoomParticipation(context.Context, string, string) error { return errDry }
func (dryWriter) StagePush(context.Context, string, []writer.PushRow) error  { return errDry }
func (dryWriter) UnstagePush(context.Context, string) error                  { return errDry }
func (dryWriter) StoreStateGroup(context.Context, string, string, int64, []writer.StateRow, func() ([]writer.StateRow, error)) (int64, error) {
	return 0, errDry
}
func (dryWriter) StoreFullStateGroup(context.Context, string, string, []writer.StateRow) (int64, error) {
	return 0, errDry
}
func (dryWriter) AddDelayedEvent(context.Context, writer.DelayedEvent, int) (int64, error) {
	return 0, errDry
}
func (dryWriter) SaveProfileJob(context.Context, writer.ProfileJob) error            { return errDry }
func (dryWriter) CheckpointProfileJob(context.Context, string, string, string) error { return errDry }
func (dryWriter) DeleteProfileJob(context.Context, string, string) error             { return errDry }
func (dryWriter) ProfileJobs(context.Context) ([]writer.ProfileJob, error)           { return nil, nil }
