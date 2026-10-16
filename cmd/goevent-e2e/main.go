// Command goevent-e2e drives goevent-creator through a real hostname, as a
// client would, and checks what comes back: status codes and error codes,
// the events as Synapse serves them afterwards, and the worker's own
// counters (shadow and native mismatches must not move).
//
// It writes to one of the allowlisted rooms only, as the test account:
//
//	GOEVENT_E2E_PASSWORD=... go run ./cmd/goevent-e2e \
//	  -base https://goevent.aguiarvieira.pt -user test \
//	  -room '!nmZrvcKkeQiFGRlJtE:aguiarvieira.pt' \
//	  -metrics http://<container ip>:9102/metrics
//
// Every event it sends says it is a test. It exits non-zero if any step
// fails.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/allowlist"
)

type client struct {
	base, token string
	http        *http.Client
}

type resp struct {
	status int
	body   gjson.Result
	raw    string
}

func (c *client) do(method, path string, body any, token bool) resp {
	var rd io.Reader
	switch b := body.(type) {
	case nil:
	case string:
		rd = strings.NewReader(b)
	default:
		j, _ := json.Marshal(b)
		rd = bytes.NewReader(j)
	}
	req, err := http.NewRequest(method, c.base+path, rd)
	if err != nil {
		return resp{raw: err.Error()}
	}
	req.Header.Set("Content-Type", "application/json")
	// What nginx adds; needed when -base is the worker itself.
	req.Header.Set("X-Forwarded-For", "127.0.0.1")
	if token && c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	r, err := c.http.Do(req)
	if err != nil {
		return resp{raw: err.Error()}
	}
	defer r.Body.Close()
	b, _ := io.ReadAll(r.Body)
	return resp{status: r.StatusCode, body: gjson.ParseBytes(b), raw: string(b)}
}

type step struct {
	name, result, detail string
}

type suite struct {
	c      *client
	room   string
	run    string
	steps  []step
	failed bool
}

func (s *suite) record(name string, ok bool, detail string, args ...any) {
	r := "PASS"
	if !ok {
		r, s.failed = "FAIL", true
	}
	s.steps = append(s.steps, step{name, r, fmt.Sprintf(detail, args...)})
	fmt.Printf("%-4s %-40s %s\n", r, name, fmt.Sprintf(detail, args...))
}

func (s *suite) skip(name, why string) {
	s.steps = append(s.steps, step{name, "SKIP", why})
	fmt.Printf("%-4s %-40s %s\n", "SKIP", name, why)
}

func esc(s string) string { return url.PathEscape(s) }

func (s *suite) roomPath(rest string) string {
	return "/_matrix/client/v3/rooms/" + esc(s.room) + rest
}

// txn makes a transaction ID unique to this run.
func (s *suite) txn(name string) string { return "goevent-e2e-" + s.run + "-" + name }

// send PUTs an event and checks it comes back as sent.
func (s *suite) send(name, typ string, content map[string]any, query string) string {
	txn := s.txn(name)
	r := s.c.do(http.MethodPut, s.roomPath("/send/"+esc(typ)+"/"+esc(txn))+query, content, true)
	id := r.body.Get("event_id").Str
	if r.status != 200 || id == "" {
		s.record(name, false, "status %d %s", r.status, r.raw)
		return ""
	}
	s.checkEvent(name, id, typ, content, txn)
	return id
}

// checkEvent reads the event back and compares what a client sees.
func (s *suite) checkEvent(name, id, typ string, content map[string]any, txn string) {
	ev := s.c.do(http.MethodGet, s.roomPath("/event/"+esc(id)), nil, true)
	if ev.status != 200 {
		s.record(name, false, "%s: read back %d %s", id, ev.status, ev.raw)
		return
	}
	var problems []string
	if ev.body.Get("type").Str != typ {
		problems = append(problems, "type "+ev.body.Get("type").Str)
	}
	want, _ := json.Marshal(content)
	if !jsonEqual(want, []byte(ev.body.Get("content").Raw)) {
		problems = append(problems, "content "+ev.body.Get("content").Raw)
	}
	// Echoed only to the device that sent it: the event carries our device
	// and transaction in its internal metadata.
	if txn != "" && ev.body.Get("unsigned.transaction_id").Str != txn {
		problems = append(problems, "transaction_id "+ev.body.Get("unsigned.transaction_id").Raw)
	}
	s.record(name, len(problems) == 0, "%s %s", id, strings.Join(problems, "; "))
}

// expectError checks a refusal: status and errcode.
func (s *suite) expectError(name string, r resp, status int, errcodes ...string) {
	ok := r.status == status && slices.Contains(errcodes, r.body.Get("errcode").Str)
	s.record(name, ok, "status %d errcode %s (want %d %v)", r.status, r.body.Get("errcode").Str, status, errcodes)
}

func jsonEqual(a, b []byte) bool {
	var x, y any
	if json.Unmarshal(a, &x) != nil || json.Unmarshal(b, &y) != nil {
		return false
	}
	ja, _ := json.Marshal(x)
	jb, _ := json.Marshal(y)
	return bytes.Equal(ja, jb)
}

// metrics is a scrape: series line -> value.
type metrics map[string]float64

func scrape(u string) (metrics, error) {
	r, err := http.Get(u)
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	b, _ := io.ReadAll(r.Body)
	m := metrics{}
	for _, line := range strings.Split(string(b), "\n") {
		if !strings.HasPrefix(line, "goevent_") {
			continue
		}
		i := strings.LastIndexByte(line, ' ')
		if i < 0 {
			continue
		}
		var v float64
		fmt.Sscan(line[i+1:], &v)
		m[line[:i]] = v
	}
	return m, nil
}

// sum adds the series of a metric whose labels contain every match.
func (m metrics) sum(name string, match ...string) float64 {
	var t float64
	for k, v := range m {
		if !strings.HasPrefix(k, name+"{") && k != name {
			continue
		}
		ok := true
		for _, s := range match {
			if !strings.Contains(k, s) {
				ok = false
			}
		}
		if ok {
			t += v
		}
	}
	return t
}

func main() { os.Exit(run()) }

func run() int {
	base := flag.String("base", "https://goevent.aguiarvieira.pt", "the test hostname")
	user := flag.String("user", "test", "localpart of the test account")
	room := flag.String("room", "!nmZrvcKkeQiFGRlJtE:aguiarvieira.pt", "an allowlisted room the account may write to")
	metricsURL := flag.String("metrics", "", "the worker's /metrics, to check its counters (optional)")
	flag.Parse()

	if !slices.Contains(allowlist.Permitted, *room) {
		fmt.Fprintf(os.Stderr, "room %s is not one the worker may write to (%v)\n", *room, allowlist.Permitted)
		return 2
	}
	password := os.Getenv("GOEVENT_E2E_PASSWORD")
	if password == "" {
		fmt.Fprintln(os.Stderr, "GOEVENT_E2E_PASSWORD is not set")
		return 2
	}

	c := &client{base: strings.TrimRight(*base, "/"), http: &http.Client{Timeout: 60 * time.Second}}
	s := &suite{c: c, room: *room, run: fmt.Sprint(time.Now().Unix())}

	var before metrics
	if *metricsURL != "" {
		var err error
		if before, err = scrape(*metricsURL); err != nil {
			fmt.Fprintln(os.Stderr, "metrics:", err)
			return 2
		}
		modes := []string{}
		for k := range before {
			if strings.HasPrefix(k, "goevent_mode_info{") {
				modes = append(modes, strings.TrimPrefix(k, "goevent_mode_info"))
			}
		}
		slices.Sort(modes)
		fmt.Println("modes:", strings.Join(modes, " "))
	}

	login := c.do(http.MethodPost, "/_matrix/client/v3/login", map[string]any{
		"type":                        "m.login.password",
		"identifier":                  map[string]any{"type": "m.id.user", "user": *user},
		"password":                    password,
		"initial_device_display_name": "goevent-e2e " + s.run,
	}, false)
	c.token = login.body.Get("access_token").Str
	if c.token == "" {
		fmt.Fprintf(os.Stderr, "login: %d %s\n", login.status, login.raw)
		return 2
	}
	me := login.body.Get("user_id").Str
	defer func() {
		r := c.do(http.MethodPost, "/_matrix/client/v3/logout", map[string]any{}, true)
		if r.status != 200 {
			fmt.Fprintf(os.Stderr, "logout: %d %s\n", r.status, r.raw)
		}
	}()
	fmt.Printf("run %s as %s in %s\n\n", s.run, me, s.room)

	tag := "goevent-e2e " + s.run

	// /send
	text := s.send("send text", "m.room.message", map[string]any{"msgtype": "m.text", "body": tag + ": text"}, "")
	if text != "" {
		r := c.do(http.MethodPut, s.roomPath("/send/m.room.message/"+esc(s.txn("send text"))), map[string]any{"msgtype": "m.text", "body": tag + ": text"}, true)
		s.record("send txn retry", r.status == 200 && r.body.Get("event_id").Str == text, "%d %s (want %s)", r.status, r.body.Get("event_id").Str, text)
	}
	post := c.do(http.MethodPost, s.roomPath("/send/m.room.message"), map[string]any{"msgtype": "m.notice", "body": tag + ": POST"}, true)
	if id := post.body.Get("event_id").Str; post.status == 200 && id != "" {
		s.checkEvent("send POST", id, "m.room.message", map[string]any{"msgtype": "m.notice", "body": tag + ": POST"}, "")
	} else {
		s.record("send POST", false, "status %d %s", post.status, post.raw)
	}
	s.send("send notice", "m.room.message", map[string]any{"msgtype": "m.notice", "body": tag + ": notice"}, "")
	s.send("send mention", "m.room.message", map[string]any{"msgtype": "m.text", "body": tag + ": mention",
		"m.mentions": map[string]any{"user_ids": []string{me}}}, "")
	if id := s.send("send sticky", "m.room.message", map[string]any{"msgtype": "m.notice", "body": tag + ": sticky"},
		"?org.matrix.msc4354.sticky_duration_ms=60000"); id != "" {
		ev := c.do(http.MethodGet, s.roomPath("/event/"+esc(id)), nil, true)
		s.record("sticky field", ev.body.Get("msc4354_sticky.duration_ms").Int() == 60000, "%s", ev.body.Get("msc4354_sticky").Raw)
	}
	s.send("send custom type", "pt.aguiarvieira.goevent.test", map[string]any{"run": s.run, "n": 1}, "")

	if text != "" {
		s.send("send reply", "m.room.message", map[string]any{"msgtype": "m.notice", "body": tag + ": reply",
			"m.relates_to": map[string]any{"m.in_reply_to": map[string]any{"event_id": text}}}, "")
		s.send("send edit", "m.room.message", map[string]any{"msgtype": "m.text", "body": "* " + tag + ": edited",
			"m.new_content": map[string]any{"msgtype": "m.text", "body": tag + ": edited"},
			"m.relates_to":  map[string]any{"rel_type": "m.replace", "event_id": text}}, "")
		s.send("send reference", "m.room.message", map[string]any{"msgtype": "m.notice", "body": tag + ": reference",
			"m.relates_to": map[string]any{"rel_type": "m.reference", "event_id": text}}, "")
		s.send("send thread", "m.room.message", map[string]any{"msgtype": "m.notice", "body": tag + ": thread",
			"m.relates_to": map[string]any{"rel_type": "m.thread", "event_id": text, "is_falling_back": true,
				"m.in_reply_to": map[string]any{"event_id": text}}}, "")
		reaction := map[string]any{"m.relates_to": map[string]any{"rel_type": "m.annotation", "event_id": text, "key": "🧪"}}
		s.send("send reaction", "m.reaction", reaction, "")
		r := c.do(http.MethodPut, s.roomPath("/send/m.reaction/"+esc(s.txn("reaction twice"))), reaction, true)
		s.expectError("send reaction twice", r, 400, "M_DUPLICATE_ANNOTATION")
	} else {
		s.skip("relations", "no text event to relate to")
	}

	// Refusals: Synapse's words, whoever serves the request.
	s.expectError("refuse non-object", c.do(http.MethodPut, s.roomPath("/send/m.room.message/"+esc(s.txn("array"))), "[1]", true), 400, "M_BAD_JSON", "M_NOT_JSON")
	s.expectError("refuse invalid JSON", c.do(http.MethodPut, s.roomPath("/send/m.room.message/"+esc(s.txn("garbage"))), "{nope", true), 400, "M_NOT_JSON")
	s.expectError("refuse message without body", c.do(http.MethodPut, s.roomPath("/send/m.room.message/"+esc(s.txn("nobody"))),
		map[string]any{"msgtype": "m.text"}, true), 400, "M_UNKNOWN")
	s.expectError("refuse unknown relation", c.do(http.MethodPut, s.roomPath("/send/m.room.message/"+esc(s.txn("unknownrel"))),
		map[string]any{"msgtype": "m.text", "body": "x", "m.relates_to": map[string]any{"rel_type": "m.replace", "event_id": "$doesnotexist"}}, true), 400, "M_UNKNOWN")
	s.expectError("refuse float", c.do(http.MethodPut, s.roomPath("/send/m.room.message/"+esc(s.txn("float"))),
		`{"msgtype":"m.text","body":"x","n":1.5}`, true), 400, "M_BAD_JSON")
	saved := c.token
	c.token = ""
	s.expectError("refuse no token", c.do(http.MethodPut, s.roomPath("/send/m.room.message/"+esc(s.txn("notoken"))), map[string]any{"msgtype": "m.text", "body": "x"}, true), 401, "M_MISSING_TOKEN")
	c.token = "syt_bogus_token"
	s.expectError("refuse bad token", c.do(http.MethodPut, s.roomPath("/send/m.room.message/"+esc(s.txn("badtoken"))), map[string]any{"msgtype": "m.text", "body": "x"}, true), 401, "M_UNKNOWN_TOKEN")
	c.token = saved

	// /state
	topic := map[string]any{"topic": tag + ": topic"}
	r := c.do(http.MethodPut, s.roomPath("/state/m.room.topic/"), topic, true)
	if id := r.body.Get("event_id").Str; r.status == 200 && id != "" {
		s.checkEvent("state topic", id, "m.room.topic", topic, "")
		again := c.do(http.MethodPut, s.roomPath("/state/m.room.topic/"), topic, true)
		s.record("state repeat is deduplicated", again.status == 200 && again.body.Get("event_id").Str == id, "%d %s (want %s)", again.status, again.body.Get("event_id").Str, id)
	} else {
		s.record("state topic", false, "status %d %s", r.status, r.raw)
	}

	// /redact
	victim := s.send("send to redact", "m.room.message", map[string]any{"msgtype": "m.notice", "body": tag + ": to be redacted"}, "")
	if victim != "" {
		r := c.do(http.MethodPut, s.roomPath("/redact/"+esc(victim)+"/"+esc(s.txn("redact"))), map[string]any{"reason": tag}, true)
		ev := c.do(http.MethodGet, s.roomPath("/event/"+esc(victim)), nil, true)
		s.record("redact", r.status == 200 && r.body.Get("event_id").Str != "" && ev.body.Get("content").Raw == "{}",
			"%d %s; redacted content %s", r.status, r.body.Get("event_id").Str, ev.body.Get("content").Raw)
	}

	// Membership: a user that does not exist, so nobody is disturbed.
	probe := "@goevent-e2e-probe:" + strings.SplitN(me, ":", 2)[1]
	r = c.do(http.MethodPost, s.roomPath("/ban"), map[string]any{"user_id": probe, "reason": tag}, true)
	s.record("ban probe", r.status == 200, "%d %s", r.status, r.raw)
	r = c.do(http.MethodPost, s.roomPath("/unban"), map[string]any{"user_id": probe}, true)
	s.record("unban probe", r.status == 200, "%d %s", r.status, r.raw)
	s.expectError("refuse kick of absent user", c.do(http.MethodPost, s.roomPath("/kick"), map[string]any{"user_id": probe}, true), 403, "M_FORBIDDEN")
	r = c.do(http.MethodPost, "/_matrix/client/v3/join/"+esc(s.room), map[string]any{}, true)
	s.record("join while joined", r.status == 200 && r.body.Get("room_id").Str == s.room, "%d %s", r.status, r.raw)

	if before != nil {
		// Checks run in the background; give them a moment.
		time.Sleep(3 * time.Second)
		after, err := scrape(*metricsURL)
		if err != nil {
			s.record("metrics", false, "%v", err)
		} else {
			d := func(name string, match ...string) float64 {
				return after.sum(name, match...) - before.sum(name, match...)
			}
			s.record("no shadow mismatches", d("goevent_shadow_checks_total", `result="mismatch"`) == 0 && d("goevent_shadow_checks_total", `result="error"`) == 0,
				"mismatch +%v, error +%v, match +%v", d("goevent_shadow_checks_total", `result="mismatch"`),
				d("goevent_shadow_checks_total", `result="error"`), d("goevent_shadow_checks_total", `result="match"`))
			s.record("no native mismatches", d("goevent_native_verify_total", `result="mismatch"`) == 0,
				"mismatch +%v, match +%v", d("goevent_native_verify_total", `result="mismatch"`), d("goevent_native_verify_total", `result="match"`))
			fmt.Printf("\nnative: created +%v, fallbacks +%v\n", d("goevent_native_total", `result="created"`), d("goevent_native_fallback_total"))
			for k, v := range after {
				if strings.HasPrefix(k, "goevent_native_fallback_total{") && v-before[k] > 0 {
					fmt.Printf("  %s +%v\n", strings.TrimPrefix(k, "goevent_native_fallback_total"), v-before[k])
				}
			}
		}
	}

	pass, fail, skip := 0, 0, 0
	for _, st := range s.steps {
		switch st.result {
		case "PASS":
			pass++
		case "FAIL":
			fail++
		default:
			skip++
		}
	}
	fmt.Printf("\n%d passed, %d failed, %d skipped\n", pass, fail, skip)
	if s.failed {
		return 1
	}
	return 0
}
