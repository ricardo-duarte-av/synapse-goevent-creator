// Command goevent-policy is a fake MSC4284 policy server, for the e2e suite's
// policy room only. It is a minimal homeserver of its own (Synapse uses a
// room's policy server only when that server is in the room): a federation
// signing key, one bot user that joins over federation, a /send that accepts
// whatever arrives, and /_matrix/policy/v1/sign, which signs every event with
// the policy key unless its body holds the spam marker, or the mode says
// otherwise.
//
// A second listener, for localhost only, controls it:
//
//	POST /mode?m=sign|refuse|empty|error   how /sign answers
//	POST /join?room=!id:server&via=server   the bot joins (make_join, send_join)
//	GET  /state                             the policy key and the mode
package main

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/tidwall/gjson"
	"go.mau.fi/util/exhttp"
	"maunium.net/go/mautrix/crypto/canonicaljson"
	"maunium.net/go/mautrix/federation"
	"maunium.net/go/mautrix/federation/pdu"
	"maunium.net/go/mautrix/id"
)

// Marker is what makes an event spam to the fake.
const Marker = "goevent-spam"

// policyKeyID is POLICY_SERVER_KEY_ID.
const policyKeyID = "ed25519:policy_server"

type server struct {
	name     string
	bot      id.UserID
	key      *federation.SigningKey
	policy   ed25519.PrivateKey
	fed      *federation.Client
	log      zerolog.Logger
	mu       sync.Mutex
	mode     string
	versions map[string]id.RoomVersion
}

func main() {
	name := flag.String("server", "policy.aguiarvieira.pt", "the fake's server name")
	listen := flag.String("listen", ":28448", "federation listener (behind the TLS reverse proxy)")
	control := flag.String("control", "127.0.0.1:28449", "control listener; keep it local")
	data := flag.String("data", "/data", "where the keys are kept")
	flag.Parse()
	log := zerolog.New(os.Stdout).With().Timestamp().Logger()

	key, err := loadKey(filepath.Join(*data, "server.key"), func() string { return federation.GenerateSigningKey().SynapseString() })
	if err != nil {
		log.Fatal().Err(err).Msg("server key")
	}
	pk, err := loadKey(filepath.Join(*data, "policy.key"), func() string {
		return "ed25519 policy_server " + base64.RawStdEncoding.EncodeToString(federation.GenerateSigningKey().Priv.Seed())
	})
	if err != nil {
		log.Fatal().Err(err).Msg("policy key")
	}
	s := &server{
		name: *name, bot: id.NewUserID("policy", *name), key: key, policy: pk.Priv,
		fed: federation.NewClient(*name, key, federation.NewInMemoryCache(), exhttp.SensibleClientSettings),
		log: log, mode: "sign", versions: map[string]id.RoomVersion{},
	}
	s.fed.UserAgent = "goevent-policy"

	mux := http.NewServeMux()
	(&federation.KeyServer{
		KeyProvider: &federation.StaticServerKey{ServerName: *name, Key: key},
		Version:     federation.ServerVersion{Name: "goevent-policy", Version: "0"},
	}).Register(mux, log)
	mux.HandleFunc("PUT /_matrix/federation/v1/send/{txn}", s.send)
	mux.HandleFunc("PUT /_matrix/federation/v2/invite/{room}/{event}", s.invite)
	mux.HandleFunc("GET /_matrix/federation/v1/query/profile", s.profile)
	mux.HandleFunc("POST /_matrix/policy/v1/sign", s.sign)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		log.Info().Str("method", r.Method).Str("path", r.URL.Path).Msg("Unhandled request")
		writeJSON(w, http.StatusNotFound, map[string]string{"errcode": "M_UNRECOGNIZED", "error": "Unrecognized request"})
	})

	cmux := http.NewServeMux()
	cmux.HandleFunc("POST /mode", func(w http.ResponseWriter, r *http.Request) {
		m := r.URL.Query().Get("m")
		switch m {
		case "sign", "refuse", "empty", "error":
		default:
			http.Error(w, "mode is sign, refuse, empty or error", http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		s.mode = m
		s.mu.Unlock()
		log.Info().Str("mode", m).Msg("Mode")
		fmt.Fprintln(w, m)
	})
	cmux.HandleFunc("POST /join", s.join)
	cmux.HandleFunc("GET /state", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		writeJSON(w, http.StatusOK, map[string]string{
			"server": s.name, "bot": s.bot.String(), "mode": s.mode,
			"policy_public_key": base64.RawStdEncoding.EncodeToString(s.policy.Public().(ed25519.PublicKey)),
		})
	})
	go func() {
		log.Fatal().Err(http.ListenAndServe(*control, cmux)).Msg("control listener")
	}()
	log.Info().Str("server", *name).Str("listen", *listen).Str("bot", s.bot.String()).Msg("Serving")
	log.Fatal().Err(http.ListenAndServe(*listen, logged(log, mux))).Msg("federation listener")
}

// loadKey reads a Synapse-format key line, writing a new one first if there
// is none.
func loadKey(path string, generate func() string) (*federation.SigningKey, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		line := generate()
		if err := os.WriteFile(path, []byte(line+"\n"), 0o600); err != nil {
			return nil, err
		}
		b = []byte(line)
	} else if err != nil {
		return nil, err
	}
	return federation.ParseSynapseKey(strings.TrimSpace(string(b)))
}

func logged(log zerolog.Logger, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := ""
		if a := r.Header.Get("Authorization"); strings.HasPrefix(a, "X-Matrix ") {
			for _, p := range strings.Split(strings.TrimPrefix(a, "X-Matrix "), ",") {
				if k, v, ok := strings.Cut(strings.TrimSpace(p), "="); ok && k == "origin" {
					origin = strings.Trim(v, `"`)
				}
			}
		}
		log.Info().Str("method", r.Method).Str("path", r.URL.Path).Str("origin", origin).Msg("Request")
		h.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	b, _ := json.Marshal(v)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(b)
}

// send accepts a transaction: every PDU is acknowledged, nothing kept.
func (s *server) send(w http.ResponseWriter, r *http.Request) {
	var txn struct {
		PDUs []json.RawMessage `json:"pdus"`
	}
	_ = json.NewDecoder(r.Body).Decode(&txn)
	out := map[string]any{}
	for _, raw := range txn.PDUs {
		v := gjson.ParseBytes(raw)
		s.log.Info().Str("type", v.Get("type").Str).Str("sender", v.Get("sender").Str).Str("room_id", v.Get("room_id").Str).
			Msg("PDU received")
	}
	writeJSON(w, http.StatusOK, map[string]any{"pdus": out})
}

// invite signs an invite of the bot, as a server accepting it does.
func (s *server) invite(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Event       json.RawMessage `json:"event"`
		RoomVersion id.RoomVersion  `json:"room_version"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"errcode": "M_BAD_JSON", "error": err.Error()})
		return
	}
	var ev pdu.PDU
	if err := json.Unmarshal(req.Event, &ev); err != nil || ev.StateKey == nil || *ev.StateKey != s.bot.String() {
		writeJSON(w, http.StatusForbidden, map[string]string{"errcode": "M_FORBIDDEN", "error": "only the bot takes invites"})
		return
	}
	if err := s.signAsServer(&ev, req.RoomVersion); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"errcode": "M_UNKNOWN", "error": err.Error()})
		return
	}
	s.mu.Lock()
	s.versions[string(ev.RoomID)] = req.RoomVersion
	s.mu.Unlock()
	s.log.Info().Str("room_id", string(ev.RoomID)).Str("sender", string(ev.Sender)).Msg("Invited")
	writeJSON(w, http.StatusOK, map[string]any{"event": &ev})
}

// signAsServer adds the server's signature, as on any event it accepts.
func (s *server) signAsServer(ev *pdu.PDU, rv id.RoomVersion) error {
	raw, err := redactedJSON(ev, rv)
	if err != nil {
		return err
	}
	ev.AddSignature(s.name, s.key.ID, base64.RawStdEncoding.EncodeToString(ed25519.Sign(s.key.Priv, raw)))
	return nil
}

// redactedJSON is what a signature covers: the event as Synapse redacts it
// (msc4354_sticky dropped, which mautrix keeps), without signatures and
// unsigned.
func redactedJSON(ev *pdu.PDU, rv id.RoomVersion) ([]byte, error) {
	c := ev.Clone()
	c.Sticky = nil
	return canonicaljson.Marshal(c.RedactForSignature(rv))
}

func (s *server) profile(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"displayname": "goevent fake policy server"})
}

// sign is /_matrix/policy/v1/sign: the policy key's signature, unless the
// event is spam or the mode says otherwise.
func (s *server) sign(w http.ResponseWriter, r *http.Request) {
	var ev pdu.PDU
	if err := json.NewDecoder(r.Body).Decode(&ev); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"errcode": "M_BAD_JSON", "error": err.Error()})
		return
	}
	s.mu.Lock()
	mode := s.mode
	rv := s.versions[string(ev.RoomID)]
	s.mu.Unlock()
	if rv == "" {
		rv = id.RoomV11
	}
	body := gjson.GetBytes(ev.Content, "body").Str
	spam := strings.Contains(body, Marker)
	l := s.log.Info().Str("type", ev.Type).Str("sender", string(ev.Sender)).Str("mode", mode).Bool("spam", spam)
	switch {
	case mode == "error":
		l.Msg("Sign: error")
		writeJSON(w, http.StatusInternalServerError, map[string]string{"errcode": "M_UNKNOWN", "error": "goevent fake policy server: error mode"})
		return
	case mode == "empty":
		l.Msg("Sign: no signature")
		writeJSON(w, http.StatusOK, map[string]any{})
		return
	case mode == "refuse" || spam:
		l.Msg("Sign: refused")
		writeJSON(w, http.StatusForbidden, map[string]string{"errcode": "M_FORBIDDEN", "error": "goevent fake policy server: probable spam"})
		return
	}
	raw, err := redactedJSON(&ev, rv)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"errcode": "M_BAD_JSON", "error": err.Error()})
		return
	}
	sig := base64.RawStdEncoding.EncodeToString(ed25519.Sign(s.policy, raw))
	l.Msg("Sign: signed")
	writeJSON(w, http.StatusOK, map[string]any{s.name: map[string]string{policyKeyID: sig}})
}

// join is make_join then send_join, through via.
func (s *server) join(w http.ResponseWriter, r *http.Request) {
	room, via := id.RoomID(r.URL.Query().Get("room")), r.URL.Query().Get("via")
	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()
	tmpl, err := s.fed.MakeJoin(ctx, &federation.ReqMakeJoin{RoomID: room, UserID: s.bot, Via: via,
		SupportedVersions: []id.RoomVersion{id.RoomV11}})
	if err != nil {
		http.Error(w, "make_join: "+err.Error(), http.StatusBadGateway)
		return
	}
	var ev pdu.PDU
	if err := json.Unmarshal(tmpl.Event, &ev); err != nil {
		http.Error(w, "make_join event: "+err.Error(), http.StatusBadGateway)
		return
	}
	ev.OriginServerTS = time.Now().UnixMilli()
	if err := ev.FillContentHash(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := s.signAsServer(&ev, tmpl.RoomVersion); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	eventID, err := ev.GetEventID(tmpl.RoomVersion)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	raw, _ := json.Marshal(&ev)
	if _, err := s.fed.SendJoin(ctx, &federation.ReqSendJoin{RoomID: room, EventID: eventID, Event: raw, Via: via}); err != nil {
		http.Error(w, "send_join: "+err.Error(), http.StatusBadGateway)
		return
	}
	s.mu.Lock()
	s.versions[string(room)] = tmpl.RoomVersion
	s.mu.Unlock()
	s.log.Info().Str("room_id", string(room)).Str("event_id", string(eventID)).Msg("Joined")
	fmt.Fprintln(w, eventID)
}
