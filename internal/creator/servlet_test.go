package creator

import (
	"strconv"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

// The answers are Python's (json_decoder.decode, int(bytes)), checked in
// Synapse's container.
func TestParseJSONObject(t *testing.T) {
	for body, want := range map[string]string{
		`{"a":1} `:          "",
		` {"a":1}`:          "",
		"\n\r\t{}\n":        "",
		"\xef\xbb\xbf{}":    "refused:not_json",
		`{"a":NaN}`:         "refused:not_json",
		`{"a":1}x`:          "refused:not_json",
		"{\"a\":\"x\x01\"}": "refused:not_json",
		"{\"a\":\"\xff\"}":  "refused:not_json",
		``:                  "refused:not_json",
		`{"a":1,}`:          "refused:not_json",
		`{'a':1}`:           "refused:not_json",
		`{"a":-}`:           "refused:not_json",
		`null`:              "refused:not_object",
		`[1]`:               "refused:not_object",
		`{"a":1e400}`:       "",
		`{"a":"\ud800"}`:    "surrogate_escape",
		`{"a":1,"a":2}`:     "duplicate_keys",
		`{"a":` + strings.Repeat("1", 5000) + `}`:           "long_integer",
		strings.Repeat("[", 101) + strings.Repeat("]", 101): "deep_json",
	} {
		_, st := parseJSONObject([]byte(body), false)
		got := ""
		if st != nil {
			got = st.fallback
			if st.ref != nil {
				got = "refused:" + st.ref.reason
			}
		}
		if got != want {
			t.Errorf("%.40q: %q, want %q", body, got, want)
		}
	}
	if b, st := parseJSONObject(nil, true); st != nil || string(b) != "{}" {
		t.Errorf("empty body with allow_empty_body: %s %+v", b, st)
	}
}

func TestPyIntBytes(t *testing.T) {
	for s, want := range map[string]int64{" 12 ": 12, "+5": 5, "-5": -5, "1_0": 10, "00012": 12, "\t7\n": 7} {
		if n, ok, exact := pyIntBytes(s); !ok || !exact || n != want {
			t.Errorf("%q: %d %v %v", s, n, ok, exact)
		}
	}
	for _, s := range []string{"_1", "1__0", "1_", "0x10", "", "12a", "--1", "+-1", "1 2"} {
		if _, ok, _ := pyIntBytes(s); ok {
			t.Errorf("%q parsed", s)
		}
	}
	if _, ok, exact := pyIntBytes("99999999999999999999"); !ok || exact {
		t.Error("a Python int beyond int64")
	}
}

func TestTargetUserID(t *testing.T) {
	for raw, want := range map[string]string{
		`"@a:example.org"`: "",
		`"nope"`:           "400 Expected UserID string to start with '@'",
		`""`:               "400 Expected UserID string to start with '@'",
		`"@nocolon"`:       "400 Expected UserID of the form '@localname:domain'",
		`[]`:               "400 Expected UserID string to start with '@'",
		`["@a:b"]`:         "400 Expected UserID string to start with '@'",
		`{}`:               "400 Expected UserID string to start with '@'",
		`{"a":1}`:          "500 Internal server error",
		`1`:                "500 Internal server error",
		`null`:             "500 Internal server error",
		`true`:             "500 Internal server error",
		`"@:example.org"`:  "fallback",
	} {
		_, st := targetUserID(gjson.Parse(raw))
		got := ""
		switch {
		case st == nil:
		case st.ref == nil:
			got = "fallback"
		default:
			got = strconv.Itoa(st.ref.status) + " " + st.ref.msg
		}
		if got != want {
			t.Errorf("%s: %q, want %q", raw, got, want)
		}
	}
}
