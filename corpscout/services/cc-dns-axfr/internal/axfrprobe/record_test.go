package axfrprobe

import "testing"

func TestAddressFindingOnlyFlagsNonPublicAddresses(t *testing.T) {
	tests := []struct {
		value string
		want  string
	}{
		{value: "93.184.216.34"},
		{value: "10.20.30.40", want: "public_dns_private_address"},
		{value: "127.0.0.1", want: "public_dns_private_address"},
		{value: "169.254.1.1", want: "public_dns_private_address"},
		{value: "100.64.0.1", want: "public_dns_private_address"},
		{value: "not-an-ip"},
	}
	for _, test := range tests {
		if got := addressFinding(test.value); got != test.want {
			t.Errorf("addressFinding(%q) = %q, want %q", test.value, got, test.want)
		}
	}
}
