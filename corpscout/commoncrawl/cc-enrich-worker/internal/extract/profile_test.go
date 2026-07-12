package extract

import (
	"testing"

	"cc-enrich-worker/internal/model"
)

func TestExtractProfile(t *testing.T) {
	body := []byte(`<html><head>
<script type="application/ld+json">
{
  "@context":"https://schema.org",
  "@type":"Organization",
  "name":"Acme Corporation",
  "legalName":"Acme Corp Ltd",
  "description":"We build rockets.",
  "url":"https://acme.com",
  "logo":"https://acme.com/logo.png",
  "telephone":"+1-555-0100",
  "email":"info@acme.com",
  "foundingDate":"1985-06-15",
  "numberOfEmployees":{"@type":"QuantitativeValue","value":250},
  "vatID":"GB123456789",
  "leiCode":"HWUPKR0MPOU8FGXBT394",
  "address":{"@type":"PostalAddress","streetAddress":"1 Rocket Rd","addressLocality":"Hawthorne","addressCountry":"US"},
  "sameAs":["https://www.linkedin.com/company/acme","https://twitter.com/acme"]
}
</script></head><body>x</body></html>`)

	p, ids := ExtractProfile(body)
	if p.Name != "Acme Corporation" || p.Description != "We build rockets." {
		t.Fatalf("name/desc wrong: %+v", p)
	}
	if p.Logo == "" || p.Phone != "+1-555-0100" || p.Email != "info@acme.com" {
		t.Fatalf("contact wrong: %+v", p)
	}
	if p.FoundingYear != 1985 || p.EmployeeCount != 250 || p.Country != "US" {
		t.Fatalf("firmographics wrong: %+v", p)
	}
	if len(p.SameAs) != 2 {
		t.Fatalf("sameAs wrong: %+v", p.SameAs)
	}

	byType := map[string]model.Identifier{}
	for _, id := range ids {
		byType[id.Type] = id
	}
	if l, ok := byType["lei"]; !ok || l.Value != "HWUPKR0MPOU8FGXBT394" || !l.Valid || l.Source != "jsonld" {
		t.Fatalf("lei id wrong: %+v", byType)
	}
	if v, ok := byType["vat"]; !ok || v.Value != "GB123456789" {
		t.Fatalf("vat id wrong: %+v", byType)
	}
}

func TestIsOrgAcceptsLocalBusinessSubtypes(t *testing.T) {
	// schema.org LocalBusiness/Organization subtypes whose names contain none of the generic
	// substrings — exactly the SMB segment this pipeline targets.
	yes := []string{
		"Dentist", "Hotel", "Attorney", "Plumber", "AutoRepair", "Physician",
		"BankOrCreditUnion", "TravelAgency", "RealEstateAgent", "Bakery", "HairSalon",
		"https://schema.org/Electrician", "schema:Winery", "MedicalBusiness",
		"Organization", "LocalBusiness", "Store", "Restaurant", "NGO",
	}
	for _, typ := range yes {
		if !isOrg(map[string]any{"@type": typ, "name": "x"}) {
			t.Errorf("isOrg(%q) = false, want true", typ)
		}
	}
	no := []string{"WebPage", "Person", "BlogPosting", "Product", "Event", "WebSite", "BreadcrumbList"}
	for _, typ := range no {
		if isOrg(map[string]any{"@type": typ, "name": "x"}) {
			t.Errorf("isOrg(%q) = true, want false", typ)
		}
	}
}

func TestExtractProfileGraphAndEmpty(t *testing.T) {
	// @graph wrapper + a non-org type that must be ignored
	body := []byte(`<script type="application/ld+json">
{"@context":"https://schema.org","@graph":[
  {"@type":"WebSite","name":"not a company"},
  {"@type":["LocalBusiness","Store"],"name":"Corner Store","address":{"addressCountry":"DE"}}
]}</script>`)
	p, _ := ExtractProfile(body)
	if p.Name != "Corner Store" || p.Country != "DE" {
		t.Fatalf("graph/localbusiness extraction wrong: %+v", p)
	}
	if e, _ := ExtractProfile([]byte(`<html><body>no json-ld here</body></html>`)); !e.Empty() {
		t.Fatalf("expected empty profile, got %+v", e)
	}
}
