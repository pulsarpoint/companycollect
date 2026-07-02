package extract

import "testing"

func TestTrackers(t *testing.T) {
	body := []byte(`
	<script async src="https://www.googletagmanager.com/gtag/js?id=G-ABC123DEF4"></script>
	<script>gtag('config','UA-12345-6');</script>
	<script>(function(w,d){})(window,document,'script','dataLayer','GTM-ABCD12');</script>
	<script>google_ad_client="ca-pub-1234567890123456";</script>
	<script>fbq('init', '123456789012345');</script>
	<script>(function(h){h._hjSettings={hjid:1234567,hjsv:6};})(window);</script>
	<script>_linkedin_partner_id = "987654";</script>
	<script>ym(88888888, "init", {});</script>
	`)
	got := map[string]string{}
	for _, id := range Trackers(body) {
		got[id.Type] = id.Value
		if !id.Valid || id.Source != "html" {
			t.Errorf("%s: valid/source wrong: %+v", id.Type, id)
		}
	}
	want := map[string]string{
		"ga": "G-ABC123DEF4", "ua": "UA-12345-6", "gtm": "GTM-ABCD12",
		"adsense": "ca-pub-1234567890123456", "fb_pixel": "123456789012345",
		"hotjar": "1234567", "linkedin_insight": "987654", "yandex_metrika": "88888888",
	}
	for typ, val := range want {
		if got[typ] != val {
			t.Errorf("%s: got %q want %q", typ, got[typ], val)
		}
	}
}

func TestTrackersNoFalsePositive(t *testing.T) {
	// stray short tokens / classes must not become ids
	body := []byte(`<div class="G-a"></div><span>UA-x</span><script>var k="deadbeef";</script>`)
	if n := len(Trackers(body)); n != 0 {
		t.Fatalf("expected 0 trackers, got %d: %+v", n, Trackers(body))
	}
}

func TestJSONLDTypes(t *testing.T) {
	body := []byte(`
	<script type="application/ld+json">{"@type":"Organization","name":"Acme"}</script>
	<script type="application/ld+json">{"@type":["Product","Offer"],"name":"Widget"}</script>
	<script type="application/ld+json">{"@type":"Organization"}</script>`)
	types := JSONLDTypes(body)
	seen := map[string]bool{}
	for _, ty := range types {
		seen[ty] = true
	}
	if !seen["Organization"] || !seen["Product"] || !seen["Offer"] || len(types) != 3 {
		t.Fatalf("distinct @types: %+v", types)
	}
}
