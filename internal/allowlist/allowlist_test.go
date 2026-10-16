package allowlist

import "testing"

func TestConfigCannotWiden(t *testing.T) {
	if _, err := New([]string{"!other:aguiarvieira.pt"}); err == nil {
		t.Fatal("a room outside Permitted was accepted")
	}
	if _, err := New([]string{"#test:aguiarvieira.pt"}); err == nil {
		t.Fatal("an alias was accepted")
	}
}

func TestCheck(t *testing.T) {
	l, err := New(Permitted[:1])
	if err != nil {
		t.Fatal(err)
	}
	if !l.Check(Permitted[0]) {
		t.Error("configured room refused")
	}
	if l.Check(Permitted[1]) {
		t.Error("permitted but unconfigured room allowed")
	}
	if l.Check("") {
		t.Error("empty room allowed")
	}
	var none *List
	if none.Check(Permitted[0]) {
		t.Error("nil list allowed a room")
	}
}
