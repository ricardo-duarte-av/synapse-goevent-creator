package shadow

import (
	"testing"

	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/pdu"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/route"
)

func TestMembershipOf(t *testing.T) {
	c := New(Options{})
	for _, tc := range []struct {
		ep             route.Endpoint
		action, body   string
		want, wantUser string
	}{
		{route.Join, "", `{}`, "join", "@me:x"},
		{route.Membership, "join", `{}`, "join", "@me:x"},
		{route.Membership, "leave", `{}`, "leave", "@me:x"},
		{route.Membership, "kick", `{"user_id":"@u:x"}`, "leave", "@u:x"},
		{route.Membership, "unban", `{"user_id":"@u:x"}`, "leave", "@u:x"},
		{route.Membership, "ban", `{"user_id":"@u:x"}`, "ban", "@u:x"},
		{route.Membership, "invite", `{"user_id":"@u:x"}`, "invite", "@u:x"},
	} {
		p := &Pending{req: route.Request{Endpoint: tc.ep, Action: tc.action}, body: []byte(tc.body), user: "@me:x"}
		m, u := c.membershipOf(p)
		if m != tc.want || u != tc.wantUser {
			t.Errorf("%s %s: got %s %s, want %s %s", tc.ep, tc.action, m, u, tc.want, tc.wantUser)
		}
	}
}

func TestExpectedRedactionContent(t *testing.T) {
	c := New(Options{})
	v11, _ := pdu.LookupRoomVersion("11")
	v10, _ := pdu.LookupRoomVersion("10")
	req := route.Request{Endpoint: route.Redact, Action: "$target"}

	got, ok := c.expectedContent(&Pending{req: req, rv: v11, body: []byte(`{"reason":"spam"}`)}, nil)
	if !ok || gjson.GetBytes(got, "redacts").Str != "$target" || gjson.GetBytes(got, "reason").Str != "spam" {
		t.Errorf("v11: %s %v", got, ok)
	}
	got, ok = c.expectedContent(&Pending{req: req, rv: v10, body: []byte(`{"reason":"spam"}`)}, nil)
	if !ok || gjson.GetBytes(got, "redacts").Exists() {
		t.Errorf("v10: %s %v", got, ok)
	}
	// with_relations is stripped by Synapse: not predicted yet.
	if _, ok := c.expectedContent(&Pending{req: req, rv: v11, body: []byte(`{"org.matrix.msc3912.with_relations":["m.thread"]}`)}, nil); ok {
		t.Error("with_relations content predicted")
	}
	// Membership content is assembled by Synapse: not predicted yet.
	if _, ok := c.expectedContent(&Pending{req: route.Request{Endpoint: route.Membership}, rv: v11, body: []byte(`{}`)}, nil); ok {
		t.Error("membership content predicted")
	}
	if _, ok := c.expectedContent(&Pending{req: route.Request{Endpoint: route.Send}, rv: v11, body: []byte(`[1]`)}, nil); ok {
		t.Error("non-object body predicted")
	}
}

func TestSameSet(t *testing.T) {
	if !sameSet([]string{"a", "b"}, []string{"b", "a"}) || sameSet([]string{"a"}, []string{"a", "a"}) || sameSet([]string{"a"}, []string{"b"}) {
		t.Error("sameSet")
	}
}
