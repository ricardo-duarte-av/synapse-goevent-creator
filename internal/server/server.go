// Package server is the front door: it classifies each request, decides
// whether it is ours, and otherwise proxies it to Synapse (main).
//
// In proxy mode nothing is parsed beyond the path, nothing is read from the
// database, and the request reaches Synapse unchanged. What the path says is
// still counted, so the traffic a native mode would take is known before it
// exists.
//
// In shadow mode the request is still proxied unchanged. For a write in an
// allowlisted room the shadow checker reads the room first and compares
// afterwards (package shadow).
package server

import (
	"net/http"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/allowlist"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/config"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/creator"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/proxy"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/route"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/shadow"
)

var (
	requests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goevent_requests_total",
		Help: "Requests by endpoint, whether they write, which rooms they name, how they were served and the status class returned.",
	}, []string{"endpoint", "write", "room", "served", "status"})

	upstreamSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "goevent_upstream_seconds",
		Help:    "Time waiting on the upstream (Synapse main), by endpoint.",
		Buckets: []float64{.01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120},
	}, []string{"endpoint"})

	modeInfo = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "goevent_mode_info",
		Help: "The configured mode of each endpoint (1 for the active mode).",
	}, []string{"endpoint", "mode"})
)

// Handler serves every request nginx sends us.
type Handler struct {
	cfg     *config.Config
	up      *proxy.Proxy
	rooms   *allowlist.List
	checker *shadow.Checker
	creator *creator.Creator
	log     zerolog.Logger
}

// New builds the front door. checker may be nil when nothing is shadowed,
// and native when nothing is native.
func New(cfg *config.Config, up *proxy.Proxy, rooms *allowlist.List, checker *shadow.Checker, native *creator.Creator, log zerolog.Logger) *Handler {
	for _, ep := range route.Endpoints {
		modeInfo.WithLabelValues(string(ep), string(cfg.ModeFor(ep))).Set(1)
	}
	return &Handler{cfg: cfg, up: up, rooms: rooms, checker: checker, creator: native, log: log}
}

// shadowCapture is how much of Synapse's answer a shadowed request keeps:
// the event ID is all that is read from it.
const shadowCapture = 4096

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	req := route.Classify(r.Method, r.URL.EscapedPath())
	served := "proxy"
	mode := h.cfg.ModeFor(req.Endpoint)
	// The one gate: nothing is written, natively or checked, outside the
	// allowlisted rooms. An alias names no room yet, so it never passes.
	ours := req.Write && h.rooms.Check(req.Room)

	if ours && mode == config.ModeNative && h.creator != nil && creator.Serves(req.Endpoint) {
		out := h.creator.Serve(w, r, req)
		if out.Fallback == "" {
			requests.WithLabelValues(string(req.Endpoint), "true", "allowlisted", "native", "native_"+out.Result).Inc()
			return
		}
		served = "fallback"
	}

	// Proxied, and in an allowlisted room also checked.
	var pending *shadow.Pending
	if ours && h.checker != nil && (mode == config.ModeShadow || mode == config.ModeNative) {
		pending = h.checker.Prepare(r.Context(), r, req)
		if served == "proxy" {
			served = "shadow"
		}
	}

	capture := int64(0)
	if pending != nil {
		capture = shadowCapture
	}
	res := h.up.Forward(w, r, capture)

	if pending != nil {
		h.checker.Check(pending, res.Status, res.Body)
	}

	upstreamSeconds.WithLabelValues(string(req.Endpoint)).Observe(res.Duration.Seconds())
	requests.WithLabelValues(string(req.Endpoint), strconv.FormatBool(req.Write),
		h.roomClass(req), served, statusClass(res)).Inc()

	if res.Err != nil {
		h.log.Warn().Err(res.Err).Str("endpoint", string(req.Endpoint)).
			Str("backend", res.Backend).Msg("Proxy failed")
	}
}

// roomClass says which rooms a request names, for the counters: whether a
// native mode could have taken it.
func (h *Handler) roomClass(req route.Request) string {
	switch {
	case req.Room == "":
		return "none"
	case req.Room[0] == '#':
		// Join and knock by alias: unknown until resolved.
		return "alias"
	case h.rooms.Check(req.Room):
		return "allowlisted"
	default:
		return "other"
	}
}

func statusClass(res proxy.Result) string {
	switch {
	case res.Canceled:
		return "canceled"
	case res.Err != nil:
		return "unreachable"
	case res.Status == 0:
		return "none"
	default:
		return strconv.Itoa(res.Status/100) + "xx"
	}
}
