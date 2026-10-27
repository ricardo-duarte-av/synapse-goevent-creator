package allowlist

import "testing"

func TestOnlyRoomIDs(t *testing.T) {
	for _, r := range []string{"#test:aguiarvieira.pt", "", "!", "@u:example.org", "!a b:example.org", "!a:"} {
		if _, err := New([]string{r}); err == nil {
			t.Errorf("%q was accepted", r)
		}
	}
	if _, err := New([]string{"!other:aguiarvieira.pt", "!goBCqT-LJRiRgPetKo34PrUe4GqYZ_R74c6SBGLwTo4"}); err != nil {
		t.Error(err)
	}
}

func TestCheck(t *testing.T) {
	l, err := New([]string{"!a:example.org"})
	if err != nil {
		t.Fatal(err)
	}
	if !l.Check("!a:example.org") {
		t.Error("configured room refused")
	}
	if l.Check("!b:example.org") {
		t.Error("unconfigured room allowed")
	}
	if l.Check("") {
		t.Error("empty room allowed")
	}
	var none *List
	if none.Check("!a:example.org") {
		t.Error("nil list allowed a room")
	}
}

// Set replaces the rooms, and a bad list leaves the old one in place.
func TestSet(t *testing.T) {
	l, _ := New([]string{"!a:example.org"})
	if err := l.Set([]string{"!b:example.org"}); err != nil {
		t.Fatal(err)
	}
	if l.Check("!a:example.org") || !l.Check("!b:example.org") {
		t.Error("not replaced")
	}
	if err := l.Set([]string{"!c:example.org", "#alias:example.org"}); err == nil {
		t.Fatal("an alias was accepted")
	}
	if l.Check("!c:example.org") || !l.Check("!b:example.org") {
		t.Error("a refused list changed the rooms")
	}
	if err := l.Set(nil); err != nil || l.Check("!b:example.org") || len(l.Rooms()) != 0 {
		t.Error("could not empty the list")
	}
}
