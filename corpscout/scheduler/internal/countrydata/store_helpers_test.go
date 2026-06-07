package countrydata

import "testing"

func requireStringPointer(t *testing.T, label string, got *string, want string) {
	t.Helper()
	if got == nil || *got != want {
		t.Fatalf("%s = %#v, want %q", label, got, want)
	}
}
