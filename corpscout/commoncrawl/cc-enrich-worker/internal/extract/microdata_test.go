package extract

import (
	"testing"
)

func TestExtractProfileMicrodata(t *testing.T) {
	body := []byte(`<html><body>
<div itemscope itemtype="http://schema.org/Dentist">
  <span itemprop="name">Smile Dental</span>
  <span itemprop="description">Family dentistry in Berlin.</span>
  <a itemprop="email" href="mailto:info@smile-dental.de">mail us</a>
  <span itemprop="telephone">+49 30 1234567</span>
  <span itemprop="vatID">DE136695976</span>
  <div itemprop="address" itemscope itemtype="http://schema.org/PostalAddress">
    <span itemprop="addressCountry">de</span>
  </div>
</div></body></html>`)
	prof, ids := ExtractProfileMicrodata(body, "https://smile-dental.de/")
	if prof.Name != "Smile Dental" || prof.Description != "Family dentistry in Berlin." {
		t.Fatalf("profile wrong: %+v", prof)
	}
	if prof.Email != "info@smile-dental.de" {
		t.Fatalf("mailto: prefix not stripped: %q", prof.Email)
	}
	if prof.Phone != "+49 30 1234567" || prof.Country != "DE" {
		t.Fatalf("phone/country wrong: %+v", prof)
	}
	if len(ids) != 1 || ids[0].Type != "vat" || ids[0].Value != "DE136695976" || ids[0].Source != "microdata" {
		t.Fatalf("vat identifier wrong: %+v", ids)
	}
}

func TestExtractProfileMicrodataSkipsPagesWithout(t *testing.T) {
	prof, ids := ExtractProfileMicrodata([]byte(`<html><body>plain page</body></html>`), "https://x.de/")
	if !prof.Empty() || len(ids) != 0 {
		t.Fatalf("expected empty result, got %+v %+v", prof, ids)
	}
}
