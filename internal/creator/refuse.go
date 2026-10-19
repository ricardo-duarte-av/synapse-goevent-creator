package creator

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"
	"unicode/utf8"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var refusals = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "goevent_native_refusals_total",
	Help: "Requests refused natively, in Synapse's words, by endpoint and reason.",
}, []string{"endpoint", "reason"})

// refusal is a SynapseError that Synapse would raise for the request, given
// here as return_json_error renders it: the status, and cs_error's errcode
// and error, plus additional_fields. A refusal is only given where every
// check Synapse makes before it has passed, so the answer is the one Synapse
// would have given.
type refusal struct {
	// reason labels it in goevent_native_refusals_total.
	reason  string
	status  int
	errcode string
	msg     string
	extra   map[string]any
	headers map[string]string
}

// stop ends a request early: a fallback (the request is Synapse's), or a
// refusal (Synapse's answer, given here).
type stop struct {
	fallback string
	ref      *refusal
}

func fb(reason string) *stop { return &stop{fallback: reason} }

func (f *refusal) stop() *stop { return &stop{ref: f} }

// end answers a stopped request.
func (c *Creator) end(w http.ResponseWriter, s *stop) Outcome {
	if s.ref != nil {
		return c.refuse(w, s.ref)
	}
	return fallback(s.fallback)
}

func (c *Creator) refuse(w http.ResponseWriter, f *refusal) Outcome {
	body := map[string]any{"errcode": f.errcode, "error": f.msg}
	for k, v := range f.extra {
		body[k] = v
	}
	b, _ := json.Marshal(body)
	for k, v := range f.headers {
		w.Header().Set(k, v)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(f.status)
	_, _ = w.Write(b)
	return Outcome{Result: "refused", Refused: f.reason}
}

// The refusals get_user_by_req makes (api/errors.py, api/auth/internal.py).
func missingToken(msg string) *refusal {
	return &refusal{reason: "missing_token", status: http.StatusUnauthorized, errcode: "M_MISSING_TOKEN", msg: msg}
}

func unknownToken(reason, msg string, softLogout bool) *refusal {
	return &refusal{reason: reason, status: http.StatusUnauthorized, errcode: "M_UNKNOWN_TOKEN", msg: msg,
		extra: map[string]any{"soft_logout": softLogout}}
}

var userLocked = &refusal{reason: "user_locked", status: http.StatusUnauthorized, errcode: "M_USER_LOCKED",
	msg: "User account has been locked", extra: map[string]any{"soft_logout": true}}

// accessToken is get_access_token_from_request. ok is false when the token
// cannot be decoded as ASCII, which is Synapse's 500.
func accessToken(r *http.Request) (token string, ref *refusal, ok bool) {
	headers := r.Header.Values("Authorization")
	q, hasQuery := r.URL.Query()["access_token"]
	var raw string
	if len(headers) > 0 {
		if hasQuery {
			return "", missingToken("Mixing Authorization headers and access_token query parameters."), true
		}
		if len(headers) > 1 {
			return "", missingToken("Too many Authorization headers."), true
		}
		parts := strings.Split(headers[0], " ")
		if parts[0] != "Bearer" || len(parts) != 2 {
			return "", missingToken("Invalid Authorization header."), true
		}
		raw = parts[1]
	} else {
		if len(q) == 0 {
			return "", missingToken("Missing access token"), true
		}
		raw = q[0]
	}
	for i := 0; i < len(raw); i++ {
		if raw[i] >= utf8.RuneSelf {
			return "", nil, false
		}
	}
	return raw, nil, true
}

// mightBeMacaroon is whether pymacaroons could deserialize the token as
// BinarySerializer does: urlsafe_b64decode, which drops what is not in the
// alphabet, then a v2 macaroon's version byte or a v1 packet's four hex
// digits of length. Synapse verifies such a token as a guest's with
// macaroon_secret_key, which this worker does not hold; anything else not in
// access_tokens is "Invalid access token passed.". Errs towards yes.
func mightBeMacaroon(token string) bool {
	if strings.HasPrefix(strings.TrimSpace(token), "{") {
		// pymacaroons' JSON serialisation.
		return true
	}
	var kept strings.Builder
	for i := 0; i < len(token); i++ {
		switch x := token[i]; {
		case x >= 'A' && x <= 'Z', x >= 'a' && x <= 'z', x >= '0' && x <= '9', x == '+', x == '/':
			kept.WriteByte(x)
		case x == '-':
			kept.WriteByte('+')
		case x == '_':
			kept.WriteByte('/')
		}
	}
	b, err := base64.RawStdEncoding.DecodeString(kept.String())
	if err != nil {
		// Python pads and is lenient about the last bits; only a length of
		// 4n+1 fails there.
		if kept.Len()%4 == 1 {
			return false
		}
		return true
	}
	if len(b) == 0 {
		return false
	}
	if b[0] == 2 {
		return true
	}
	if len(b) < 4 {
		return false
	}
	for _, x := range b[:4] {
		if !(x >= '0' && x <= '9' || x >= 'a' && x <= 'f' || x >= 'A' && x <= 'F') {
			return false
		}
	}
	return true
}

// internalError is an exception that is not a SynapseError: Synapse's 500.
func internalError(reason string) *refusal {
	return &refusal{reason: reason, status: http.StatusInternalServerError, errcode: "M_UNKNOWN", msg: "Internal server error"}
}

// badRequest is SynapseError(400, msg): M_UNKNOWN unless given.
func badRequest(reason, msg string, errcode ...string) *refusal {
	code := "M_UNKNOWN"
	if len(errcode) > 0 {
		code = errcode[0]
	}
	return &refusal{reason: reason, status: http.StatusBadRequest, errcode: code, msg: msg}
}

// forbidden is AuthError(403, msg) or SynapseError(403, msg, M_FORBIDDEN).
func forbidden(reason, msg string) *refusal {
	return &refusal{reason: reason, status: http.StatusForbidden, errcode: "M_FORBIDDEN", msg: msg}
}
