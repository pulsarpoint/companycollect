package extract

import (
	"reflect"
	"testing"
)

func TestExtractJSONLDPreservesEveryEntityDeterministically(t *testing.T) {
	body := []byte(`<html><head>
<script type="application/ld+json">{
  "@context":{"company":{"@id":"https://schema.org/Organization"}},
  "@type":"WebPage",
  "@id":"https://example.com/#page",
  "publisher":{"@type":"Organization","@id":"#publisher","name":"Example Ltd","email":"hello@example.com"},
  "author":{"@type":"Organization","@id":"#author","name":"Editorial Team","sameAs":["https://b.example","https://a.example","https://a.example"]}
}</script>
<script type="application/ld+json">[
  {"@type":["Store","LocalBusiness","Store"],"name":"Example Shop","legalName":"Example Shop GmbH","vatID":"DE136695976","address":{"addressCountry":"DE"}},
  {"@id":"#reference"}
]</script></head></html>`)

	entities, identifiers := ExtractJSONLD(body)
	if len(entities) != 5 {
		t.Fatalf("entities = %d, want 5: %+v", len(entities), entities)
	}
	wantLocations := []struct {
		script uint32
		path   string
		name   string
	}{
		{0, "", ""},
		{0, "/author", "Editorial Team"},
		{0, "/publisher", "Example Ltd"},
		{1, "/0", "Example Shop"},
		{1, "/1", ""},
	}
	for i, want := range wantLocations {
		got := entities[i]
		if got.ScriptIndex != want.script || got.EntityPath != want.path || got.Name != want.name {
			t.Fatalf("entity %d = script %d path %q name %q, want %+v", i, got.ScriptIndex, got.EntityPath, got.Name, want)
		}
	}
	if got := entities[1].SameAs; !reflect.DeepEqual(got, []string{"https://a.example", "https://b.example"}) {
		t.Fatalf("same_as not sorted/deduped: %v", got)
	}
	if got := entities[3].Types; !reflect.DeepEqual(got, []string{"LocalBusiness", "Store"}) {
		t.Fatalf("types not sorted/deduped: %v", got)
	}
	if entities[0].IsOrganization || !entities[1].IsOrganization || !entities[2].IsOrganization || !entities[3].IsOrganization {
		t.Fatalf("organization classification wrong: %+v", entities)
	}
	if entities[3].LegalName != "Example Shop GmbH" || entities[3].Country != "DE" {
		t.Fatalf("entity fields lost: %+v", entities[3])
	}
	if len(identifiers) != 1 || identifiers[0].Type != "vat" || identifiers[0].Value != "DE136695976" {
		t.Fatalf("structured identifiers lost: %+v", identifiers)
	}
	if got := JSONLDTypeNames(entities); !reflect.DeepEqual(got, []string{"LocalBusiness", "Organization", "Store", "WebPage"}) {
		t.Fatalf("type summary = %v", got)
	}

	for range 100 {
		again, againIDs := ExtractJSONLD(body)
		if !reflect.DeepEqual(again, entities) || !reflect.DeepEqual(againIDs, identifiers) {
			t.Fatal("identical JSON-LD produced different entities or identifiers")
		}
	}
}

func TestExtractJSONLDSkipsInvalidBlocksAndContextDefinitions(t *testing.T) {
	body := []byte(`<script type="application/ld+json">{broken</script>
<script type="application/ld+json">{
  "@context":{"Organization":{"@id":"https://schema.org/Organization"}},
  "@type":"Organization",
  "name":"Acme"
}</script>`)
	entities, _ := ExtractJSONLD(body)
	if len(entities) != 1 || entities[0].Name != "Acme" || entities[0].EntityPath != "" {
		t.Fatalf("unexpected entities: %+v", entities)
	}
}

func TestIsOrgAcceptsLocalBusinessSubtypes(t *testing.T) {
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
