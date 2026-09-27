package definitions

import (
	"errors"
	"fmt"
	"net/netip"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"

	"provider_recon/internal/matcher"
	"provider_recon/internal/model"
)

// ParamValidator checks a collector's params; feeds.Collector satisfies it.
type ParamValidator interface {
	ValidateParams(params map[string]string) error
}

var (
	categories     = []string{"cdn", "cloud", "dns", "email", "hosting", "other", "paas", "saas", "security"}
	dnsRecordTypes = []string{"A", "AAAA", "CAA", "CNAME", "HTTPS", "MX", "NS", "SRV", "TXT"}
	dnsMatchFields = []string{"all", "name", "priority", "target", "value"}
	httpParts      = []string{"body", "cookie", "header", "redirect_location", "status", "title", "tls_alpn"}
	identityTypes  = []string{"issuer_cn", "issuer_org", "san_suffix", "subject_cn"}
	slugRE         = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*[a-z0-9]$`)
	serviceNameRE  = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*$`)
	providerKeyRE  = regexp.MustCompile(`^[a-z0-9*][a-z0-9.*-]*[a-z0-9*]$`)
	countryRE      = regexp.MustCompile(`^[A-Z]{2}$`)
)

type problems struct{ errs []error }

func (p *problems) add(d *Definition, path, format string, args ...any) {
	p.errs = append(p.errs, fmt.Errorf("%s: %s: %s", d.File, path, fmt.Sprintf(format, args...)))
}

// Validate normalizes defs in place and returns every problem found, joined.
func Validate(defs []Definition, collectors map[string]ParamValidator) error {
	var p problems
	slugs := map[string]string{}
	keyOwner := map[string]string{}
	for i := range defs {
		d := &defs[i]
		validateProvider(d, &p, collectors)
		if other, dup := slugs[d.Slug]; dup {
			p.add(d, "slug", "duplicate slug %q (also in %s)", d.Slug, other)
		}
		slugs[d.Slug] = d.File
		for _, k := range d.ProviderKeys {
			if owner, taken := keyOwner[k]; taken && owner != d.Slug {
				p.add(d, "provider_keys", "key %q also claimed by %s", k, owner)
				continue
			}
			keyOwner[k] = d.Slug
		}
	}
	return errors.Join(p.errs...)
}

func validateProvider(d *Definition, p *problems, collectors map[string]ParamValidator) {
	d.Slug = strings.TrimSpace(d.Slug)
	d.DisplayName = strings.TrimSpace(d.DisplayName)
	d.Category = strings.ToLower(strings.TrimSpace(d.Category))
	d.Country = strings.ToUpper(strings.TrimSpace(d.Country))
	d.Website = strings.TrimSpace(d.Website)

	if !slugRE.MatchString(d.Slug) {
		p.add(d, "slug", "must match %s", slugRE)
	}
	if d.File != "" && filepath.Base(d.File) != d.Slug+".yaml" {
		p.add(d, "slug", "file must be named %s.yaml", d.Slug)
	}
	if d.DisplayName == "" {
		p.add(d, "display_name", "is required")
	}
	if !slices.Contains(categories, d.Category) {
		p.add(d, "category", "unknown category %q (one of %s)", d.Category, strings.Join(categories, ", "))
	}
	if d.Country != "" && !countryRE.MatchString(d.Country) {
		p.add(d, "country", "must be an ISO 3166-1 alpha-2 code")
	}
	d.Aliases = uniqueFold(d.Aliases)

	keys := make([]string, 0, len(d.ProviderKeys))
	for i, k := range d.ProviderKeys {
		k = strings.TrimSuffix(strings.ToLower(strings.TrimSpace(k)), ".")
		if !providerKeyRE.MatchString(k) {
			p.add(d, fmt.Sprintf("provider_keys[%d]", i), "invalid provider key %q", k)
		}
		keys = append(keys, k)
	}
	slices.Sort(keys)
	d.ProviderKeys = slices.Compact(keys)

	if len(d.Services) == 0 {
		p.add(d, "services", "at least one service is required")
	}
	serviceKeys := map[string]bool{}
	for i := range d.Services {
		s := &d.Services[i]
		path := fmt.Sprintf("services[%d]", i)
		s.Key = strings.ToLower(strings.TrimSpace(s.Key))
		prefix := d.Slug + "."
		if !strings.HasPrefix(s.Key, prefix) || !serviceNameRE.MatchString(strings.TrimPrefix(s.Key, prefix)) {
			p.add(d, path+".key", "service key %q must start with %q followed by [a-z0-9-]", s.Key, prefix)
		}
		if serviceKeys[s.Key] {
			p.add(d, path+".key", "duplicate service key %q", s.Key)
		}
		serviceKeys[s.Key] = true
		if strings.TrimSpace(s.DisplayName) == "" {
			p.add(d, path+".display_name", "is required")
		}
		validateService(d, s, path, p)
	}

	feedIDs := map[string]bool{}
	for i := range d.Feeds {
		path := fmt.Sprintf("feeds[%d]", i)
		validateFeed(d, &d.Feeds[i], path, serviceKeys, collectors, p)
		id := d.Feeds[i].ID()
		if feedIDs[id] {
			p.add(d, path, "duplicate feed %q; merge the tag_maps into one entry", id)
		}
		feedIDs[id] = true
	}
}

func validateService(d *Definition, s *ServiceDef, path string, p *problems) {
	if len(s.ServiceTypes) == 0 {
		p.add(d, path+".service_types", "at least one service type is required")
	}
	for i, t := range s.ServiceTypes {
		s.ServiceTypes[i] = strings.ToLower(strings.TrimSpace(t))
		if !model.Contains(model.ServiceTypes, s.ServiceTypes[i]) {
			p.add(d, path+".service_types", "unknown service type %q", s.ServiceTypes[i])
		}
	}
	for i, t := range s.Traits {
		s.Traits[i] = strings.ToLower(strings.TrimSpace(t))
		if !model.Contains(model.Traits, s.Traits[i]) {
			p.add(d, path+".traits", "unknown trait %q", s.Traits[i])
		}
	}
	for i := range s.IPRanges {
		r := &s.IPRanges[i]
		rp := fmt.Sprintf("%s.ip_ranges[%d]", path, i)
		pfx, err := netip.ParsePrefix(strings.TrimSpace(r.CIDR))
		switch {
		case err != nil:
			p.add(d, rp, "invalid cidr %q: %v", r.CIDR, err)
		case pfx != pfx.Masked():
			p.add(d, rp, "cidr %q has host bits set; did you mean %s", r.CIDR, pfx.Masked())
		default:
			r.CIDR = pfx.String()
		}
		checkConfidence(d, rp, r.Confidence, p)
	}
	for i := range s.ASNs {
		rp := fmt.Sprintf("%s.asns[%d]", path, i)
		if s.ASNs[i].ASN == 0 {
			p.add(d, rp, "asn must be > 0")
		}
		checkConfidence(d, rp, s.ASNs[i].Confidence, p)
	}
	for i := range s.DNSRules {
		r := &s.DNSRules[i]
		rp := fmt.Sprintf("%s.dns_rules[%d]", path, i)
		r.RecordType = strings.ToUpper(strings.TrimSpace(r.RecordType))
		r.MatchField = strings.ToLower(strings.TrimSpace(r.MatchField))
		if !slices.Contains(dnsRecordTypes, r.RecordType) {
			p.add(d, rp, "unsupported record_type %q", r.RecordType)
		}
		if !slices.Contains(dnsMatchFields, r.MatchField) {
			p.add(d, rp, "unsupported match_field %q", r.MatchField)
		}
		r.MatcherType, r.Pattern = checkMatcher(d, rp, r.MatcherType, r.Pattern, r.CaseSensitive, p)
		checkConfidence(d, rp, r.Confidence, p)
		if r.Priority == 0 {
			r.Priority = 100
		}
	}
	for i := range s.HTTPRules {
		r := &s.HTTPRules[i]
		rp := fmt.Sprintf("%s.http_rules[%d]", path, i)
		r.HTTPPart = strings.ToLower(strings.TrimSpace(r.HTTPPart))
		r.HeaderName = strings.ToLower(strings.TrimSpace(r.HeaderName))
		r.PathScope = strings.TrimSpace(r.PathScope)
		if !slices.Contains(httpParts, r.HTTPPart) {
			p.add(d, rp, "unsupported http_part %q", r.HTTPPart)
		}
		if r.HTTPPart == "header" && r.HeaderName == "" {
			p.add(d, rp, "header_name is required for header rules")
		}
		if r.HTTPPart != "header" && r.HeaderName != "" {
			p.add(d, rp, "header_name only applies to http_part header")
		}
		r.MatcherType, r.Pattern = checkMatcher(d, rp, r.MatcherType, r.Pattern, r.CaseSensitive, p)
		checkConfidence(d, rp, r.Confidence, p)
		if r.Priority == 0 {
			r.Priority = 100
		}
	}
	for i := range s.PTRRules {
		r := &s.PTRRules[i]
		rp := fmt.Sprintf("%s.ptr_rules[%d]", path, i)
		if strings.EqualFold(strings.TrimSpace(r.MatcherType), "exists") {
			p.add(d, rp, "exists is not meaningful for ptr rules")
		}
		r.MatcherType, r.Pattern = checkMatcher(d, rp, r.MatcherType, r.Pattern, false, p)
		checkConfidence(d, rp, r.Confidence, p)
	}
	for i := range s.CertificateIdentities {
		c := &s.CertificateIdentities[i]
		rp := fmt.Sprintf("%s.certificate_identities[%d]", path, i)
		c.IdentityType = strings.ToLower(strings.TrimSpace(c.IdentityType))
		c.IdentityValue = strings.TrimSpace(c.IdentityValue)
		if !slices.Contains(identityTypes, c.IdentityType) {
			p.add(d, rp, "unsupported identity_type %q", c.IdentityType)
		}
		if c.IdentityValue == "" {
			p.add(d, rp, "identity_value is required")
		}
		checkConfidence(d, rp, c.Confidence, p)
	}
}

// checkMatcher validates via matcher.Compile and returns the normalized
// matcher type and pattern. Regex patterns are kept verbatim; other patterns
// are trimmed, trailing-dot stripped and lowercased unless case-sensitive.
func checkMatcher(d *Definition, path, kind, pattern string, caseSensitive bool, p *problems) (string, string) {
	kind = strings.ToLower(strings.TrimSpace(kind))
	if _, err := matcher.Compile(kind, pattern, caseSensitive); err != nil {
		p.add(d, path, "%v", err)
		return kind, pattern
	}
	if kind == "regex" {
		return kind, strings.TrimSpace(pattern)
	}
	pattern = strings.TrimSuffix(strings.TrimSpace(pattern), ".")
	if !caseSensitive {
		pattern = strings.ToLower(pattern)
	}
	return kind, pattern
}

func checkConfidence(d *Definition, path string, c *float64, p *problems) {
	if c != nil && (*c < 0 || *c > 1) {
		p.add(d, path, "confidence must be between 0 and 1")
	}
}

func validateFeed(d *Definition, f *FeedRef, path string, services map[string]bool, collectors map[string]ParamValidator, p *problems) {
	f.Collector = strings.TrimSpace(f.Collector)
	c, known := collectors[f.Collector]
	if !known {
		p.add(d, path+".collector", "unknown collector %q", f.Collector)
	} else if err := c.ValidateParams(f.Params); err != nil {
		p.add(d, path+".params", "%v", err)
	}
	if len(f.TagMap) == 0 {
		p.add(d, path+".tag_map", "at least one tag mapping is required")
	}
	tags := make([]string, 0, len(f.TagMap))
	for tag := range f.TagMap {
		tags = append(tags, tag)
	}
	sort.Strings(tags)
	for _, tag := range tags {
		svc := strings.ToLower(strings.TrimSpace(f.TagMap[tag]))
		f.TagMap[tag] = svc
		if svc != "" && !services[svc] {
			p.add(d, path, "tag_map[%q] points to unknown service %q", tag, svc)
		}
	}
	if f.RemovalGraceDays < 0 || f.RemovalGraceDays > 365 {
		p.add(d, path+".removal_grace_days", "must be between 1 and 365 (omit for the default %d)", DefaultRemovalGraceDays)
	}
	for _, g := range f.GenericTags {
		if strings.Contains(g, "*") {
			continue
		}
		if _, ok := f.ResolveTag(g); !ok {
			p.add(d, path+".generic_tags", "generic tag %q is not mapped by tag_map", g)
		}
	}
}

// uniqueFold trims, drops empties, de-duplicates case-insensitively (first
// spelling wins) and sorts.
func uniqueFold(values []string) []string {
	seen := map[string]bool{}
	out := []string{}
	for _, v := range values {
		v = strings.TrimSpace(v)
		k := strings.ToLower(v)
		if v == "" || seen[k] {
			continue
		}
		seen[k] = true
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}
