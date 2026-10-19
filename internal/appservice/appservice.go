// Package appservice reads the application service registrations Synapse is
// configured with, for the one thing this worker needs from them: which users
// an appservice claims exclusively. Synapse never computes push actions for
// those users (bulk_push_rule_evaluator.py:193).
package appservice

import (
	"fmt"
	"os"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

type registration struct {
	ASToken    string `yaml:"as_token"`
	Namespaces struct {
		Users []struct {
			Exclusive bool   `yaml:"exclusive"`
			Regex     string `yaml:"regex"`
		} `yaml:"users"`
	} `yaml:"namespaces"`
}

// ExclusiveUsers is _make_exclusive_regex over the registrations: the
// exclusive user namespaces joined, matched as Python's re.match does -- at
// the start of the user ID, not necessarily to its end. nil when none.
func ExclusiveUsers(files []string) (*regexp.Regexp, error) {
	var parts []string
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			return nil, fmt.Errorf("appservice: %w", err)
		}
		var r registration
		if err := yaml.Unmarshal(b, &r); err != nil {
			return nil, fmt.Errorf("appservice: %s: %w", f, err)
		}
		for _, u := range r.Namespaces.Users {
			if u.Exclusive && u.Regex != "" {
				parts = append(parts, "("+u.Regex+")")
			}
		}
	}
	if len(parts) == 0 {
		return nil, nil
	}
	re, err := regexp.Compile(`^(?:` + strings.Join(parts, "|") + `)`)
	if err != nil {
		// A Python regex Go cannot compile (lookarounds, say).
		return nil, fmt.Errorf("appservice: exclusive user regex: %w", err)
	}
	return re, nil
}

// Tokens are the registrations' as_tokens: get_app_service_by_token's
// lookup, which get_user_by_req makes before the access_tokens table.
func Tokens(files []string) (map[string]bool, error) {
	out := map[string]bool{}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			return nil, fmt.Errorf("appservice: %w", err)
		}
		var r registration
		if err := yaml.Unmarshal(b, &r); err != nil {
			return nil, fmt.Errorf("appservice: %s: %w", f, err)
		}
		if r.ASToken != "" {
			out[r.ASToken] = true
		}
	}
	return out, nil
}
