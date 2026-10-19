package creator

import (
	"regexp"
	"strconv"
	"unicode/utf8"

	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/acl"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/pdu"
)

// The content checks of EventValidator.validate_new (events/validator.py,
// v1.163.0) for the types that have one. Each answers "" only when the
// content certainly passes Synapse's check; anything Synapse would refuse, or
// that Python would handle in a way not reproduced here, is a reason, and
// the request is Synapse's.

// canonicalJSONMaxInt is CANONICALJSON_MAX_INT; the minimum is its negative.
const canonicalJSONMaxInt = 1<<53 - 1

// maxAliasLength is MAX_ALIAS_LENGTH (api/constants.py).
const maxAliasLength = 255

var jsonInt = regexp.MustCompile(`^-?(0|[1-9][0-9]*)$`)

// isSchemaInt is POWER_LEVELS_SCHEMA's "int": a JSON integer (never a bool,
// a float or a string) within canonical JSON's range.
func isSchemaInt(v gjson.Result) bool {
	if v.Type != gjson.Number || !jsonInt.MatchString(v.Raw) {
		return false
	}
	n, err := strconv.ParseInt(v.Raw, 10, 64)
	return err == nil && n >= -canonicalJSONMaxInt && n <= canonicalJSONMaxInt
}

// validatePowerLevels is POWER_LEVELS_SCHEMA: each listed key, when present,
// an int, or an object of ints. Other keys are not checked.
func validatePowerLevels(content gjson.Result) string {
	// jsonschema checks a property that is present, null included.
	for _, k := range []string{"ban", "events_default", "invite", "kick", "redact", "state_default", "users_default"} {
		if v, ok := pdu.Field(content, k); ok && !isSchemaInt(v) {
			return "invalid_power_levels"
		}
	}
	for _, k := range []string{"events", "notifications", "users"} {
		v, ok := pdu.Field(content, k)
		if !ok {
			continue
		}
		if !v.IsObject() {
			return "invalid_power_levels"
		}
		ok = true
		v.ForEach(func(_, x gjson.Result) bool {
			ok = isSchemaInt(x)
			return ok
		})
		if !ok {
			return "invalid_power_levels"
		}
	}
	return ""
}

// validateServerACL is validate_new's refusal of an ACL that denies the
// local server (server_acl_evaluator_from_event, then
// server_matches_acl_event).
func validateServerACL(raw []byte, serverName string) string {
	a, err := acl.ParseServerACL([]byte(`{"content":` + string(raw) + `}`))
	if err != nil {
		return "invalid_server_acl"
	}
	if !a.Allowed(serverName) {
		return "acl_denies_us"
	}
	return ""
}

// validateAliases is validate_new's length check: each alias at most
// MAX_ALIAS_LENGTH characters. Python iterates whatever "aliases" is; only a
// list of strings is served here.
func validateAliases(content gjson.Result) string {
	// `if "aliases" in event.content`: a null is iterated, and raises.
	v, ok := pdu.Field(content, "aliases")
	if !ok {
		return ""
	}
	if !v.IsArray() {
		return "invalid_aliases"
	}
	for _, a := range v.Array() {
		if a.Type != gjson.String {
			return "invalid_aliases"
		}
		if utf8.RuneCountInString(a.Str) > maxAliasLength {
			return "alias_too_long"
		}
	}
	return ""
}

// validateRetention is _validate_retention: a state event, with integer
// min_lifetime and max_lifetime when given, and min <= max.
func validateRetention(state bool, content gjson.Result) string {
	if !state {
		return "retention_not_state"
	}
	lifetime := func(k string) (int64, bool, bool) {
		v := pdu.Get(content, k)
		if !v.Exists() || v.Type == gjson.Null {
			return 0, false, true
		}
		// type(x) is not int: bools and floats are refused.
		if v.Type != gjson.Number || !jsonInt.MatchString(v.Raw) {
			return 0, true, false
		}
		n, err := strconv.ParseInt(v.Raw, 10, 64)
		return n, true, err == nil
	}
	lo, hasLo, okLo := lifetime("min_lifetime")
	hi, hasHi, okHi := lifetime("max_lifetime")
	if !okLo || !okHi {
		return "invalid_retention"
	}
	if hasLo && hasHi && lo > hi {
		return "invalid_retention"
	}
	return ""
}

// validateTombstone is validate_builder's: replacement_room present, and not
// this room.
func validateTombstone(content gjson.Result, room string) string {
	r, ok := pdu.Field(content, "replacement_room")
	if !ok {
		return "invalid_tombstone"
	}
	if r.Type == gjson.String && r.Str == room {
		return "invalid_tombstone"
	}
	if r.Type != gjson.String {
		// Compared with == to the room ID: anything else passes in Synapse,
		// but is no room to point clients at.
		return "invalid_tombstone"
	}
	return ""
}
