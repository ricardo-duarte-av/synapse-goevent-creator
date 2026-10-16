package creator

import (
	"bytes"
	"context"
	"time"

	"github.com/rs/zerolog"
	"github.com/tidwall/gjson"
	"maunium.net/go/mautrix/crypto/canonicaljson"

	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/pushcalc"
)

// pending is a native event to verify.
type pending struct{ p *prepared }

// check compares a native event with what the events writer persisted: the
// event itself, its state group, its transaction mapping and its push
// actions. A mismatch means the writer did something with our event that we
// did not expect, and is logged with both sides.
func (c *Creator) check(v pending) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	p := v.p
	log := c.o.Log.With().Str("event_id", p.ev.ID).Str("room_id", p.req.Room).Logger()
	results := map[string]string{}
	record := func(check string, ok bool, detail func(*zerolog.Event)) {
		result := "match"
		if !ok {
			result = "mismatch"
			e := log.Warn().Str("check", check)
			if detail != nil {
				detail(e)
			}
			e.Msg("Native event differs from what the events writer persisted")
		}
		results[check] = result
		verifies.WithLabelValues(check, result).Inc()
	}

	stored, bad, err := c.o.Store.GetStoredEvents(ctx, []string{p.ev.ID})
	se := stored[p.ev.ID]
	if err != nil || bad[p.ev.ID] != nil || se == nil {
		record("persisted", false, func(e *zerolog.Event) { e.AnErr("err", err).AnErr("parse", bad[p.ev.ID]) })
		return
	}
	record("persisted", !se.Outlier && se.Reason == "", func(e *zerolog.Event) {
		e.Bool("outlier", se.Outlier).Str("rejected", se.Reason)
	})

	// events.rejection_reason is written from the context as sent: it must
	// be NULL, as for every event Synapse creates (a False once became the
	// text 'false' there, 2026-10-16).
	col, err := c.o.Store.RejectionColumn(ctx, p.ev.ID)
	record("rejection_column", err == nil && col == nil, func(e *zerolog.Event) {
		e.AnErr("err", err)
		if col != nil {
			e.Str("stored", *col)
		}
	})

	// The stored event, unsigned aside, is ours byte for byte once canonical.
	a, errA := canonicalWithout(se.JSON, "unsigned")
	b, errB := canonicalWithout(p.built.JSON, "unsigned")
	record("event_json", errA == nil && errB == nil && bytes.Equal(a, b), func(e *zerolog.Event) {
		e.RawJSON("stored", se.JSON).RawJSON("ours", p.built.JSON)
	})

	g, err := c.o.Store.StateGroupForEvent(ctx, p.ev.ID)
	record("state_group", err == nil && g == p.group, func(e *zerolog.Event) {
		e.AnErr("err", err).Int64("stored", g).Int64("ours", p.group)
	})

	if p.req.TxnID != "" {
		id, err := c.o.Store.TxnEvent(ctx, p.req.Room, p.rq.UserID, p.rq.DeviceID, p.req.TxnID)
		record("txn_mapping", err == nil && id == p.ev.ID, func(e *zerolog.Event) { e.AnErr("err", err).Str("stored", id) })
	}

	if c.o.Push != nil {
		pv := c.o.Push.Check(ctx, p.ev.ID, p.before, p.maxStream, p.push)
		switch pv.Outcome {
		case pushcalc.OutcomeMatch:
			record("push", true, nil)
		case pushcalc.OutcomeMismatch:
			record("push", false, func(e *zerolog.Event) { e.Strs("diff", pv.Diff) })
		default:
			results["push"] = pv.Outcome + ": " + pv.Reason
			verifies.WithLabelValues("push", pv.Outcome).Inc()
		}
	}

	log.Info().Interface("results", results).Msg("Native event verified")
}

// canonicalWithout is the canonical JSON of an event without some keys.
func canonicalWithout(raw []byte, drop ...string) ([]byte, error) {
	j := gjson.ParseBytes(raw)
	obj := map[string]canonicalRaw{}
	j.ForEach(func(k, v gjson.Result) bool {
		for _, d := range drop {
			if k.Str == d {
				return true
			}
		}
		obj[k.Str] = canonicalRaw(v.Raw)
		return true
	})
	out, err := canonicaljson.Marshal(obj)
	return out, err
}

type canonicalRaw []byte

func (r canonicalRaw) MarshalJSON() ([]byte, error) { return r, nil }
