// Package server is the front door: it classifies each request, decides
// whether it is ours, and otherwise proxies it to the Python event_creator.
//
// In proxy mode nothing is parsed beyond the path, nothing is read from the
// database, and the request reaches Python unchanged. What the path says is
// still counted, so the traffic a native mode would take is known before it
// exists.
package server

import (
	"net/http"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/allowlist"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/config"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/proxy"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/route"
)

var (
	requests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goevent_requests_total",
		Help: "Requests by endpoint, whether they write, which rooms they name, how they were served and the status class returned.",
	}, []string{"endpoint", "write", "room", "served", "status"})

	upstreamSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "goevent_upstream_seconds",
		Help:    "Time waiting on the Python event_creator, by endpoint.",
		Buckets: []float64{.01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120},
	}, []string{"endpoint"})

	modeInfo = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "goevent_mode_info",
		Help: "The configured mode of each endpoint (1 for the active mode).",
	}, []string{"endpoint", "mode"})
)

// Handler serves every request nginx sends us.
type Handler struct {
	cfg   *config.Config
	up    *proxy.Proxy
	rooms *allowlist.List
	log   zerolog.Logger
}

// New builds the front door.
func New(cfg *config.Config, up *proxy.Proxy, rooms *allowlist.List, log zerolog.Logger) *Handler {
	for _, ep := range route.Endpoints {
		modeInfo.WithLabelValues(string(ep), string(cfg.ModeFor(ep))).Set(1)
	}
	return &Handler{cfg: cfg, up: up, rooms: rooms, log: log}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	req := route.Classify(r.Method, r.URL.EscapedPath())

	// Every endpoint is proxy-only so far; config refuses any other mode.
	res := h.up.Forward(w, r, 0)

	upstreamSeconds.WithLabelValues(string(req.Endpoint)).Observe(res.Duration.Seconds())
	requests.WithLabelValues(string(req.Endpoint), strconv.FormatBool(req.Write),
		h.roomClass(req), "proxy", statusClass(res)).Inc()

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
