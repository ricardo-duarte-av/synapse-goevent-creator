package synapsecfg

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

// Auth is what get_user_by_req depends on beyond the database
// (api/auth/internal.py, config/auth.py, config/mas.py, v1.163.0).
type Auth struct {
	// Delegated is authentication by an OAuth server (MAS, or MSC3861):
	// access tokens then live outside Synapse's database.
	Delegated bool
	// AccountValidity is account_validity.enabled: requests from an expired
	// account are refused.
	AccountValidity bool
	// TrackAppserviceUserIPs and TrackPuppetedUserIPs are
	// track_appservice_user_ips and track_puppeted_user_ips.
	TrackAppserviceUserIPs, TrackPuppetedUserIPs bool
}

type authRaw struct {
	MAS                    yaml.Node `yaml:"matrix_authentication_service"`
	AccountValidity        yaml.Node `yaml:"account_validity"`
	TrackAppserviceUserIPs *bool     `yaml:"track_appservice_user_ips"`
	TrackPuppetedUserIPs   *bool     `yaml:"track_puppeted_user_ips"`
}

func authSettings(b []byte, experimental map[string]any) (Auth, error) {
	var r authRaw
	if err := yaml.Unmarshal(b, &r); err != nil {
		return Auth{}, fmt.Errorf("synapsecfg: auth settings: %w", err)
	}
	a := Auth{TrackAppserviceUserIPs: deref(r.TrackAppserviceUserIPs, false), TrackPuppetedUserIPs: deref(r.TrackPuppetedUserIPs, false)}
	if v, _ := nodeMap(&r.MAS)["enabled"].(bool); v {
		a.Delegated = true
	}
	if m, ok := experimental["msc3861"].(map[string]any); ok {
		if v, _ := m["enabled"].(bool); v {
			a.Delegated = true
		}
	}
	if v, _ := nodeMap(&r.AccountValidity)["enabled"].(bool); v {
		a.AccountValidity = true
	}
	return a, nil
}
