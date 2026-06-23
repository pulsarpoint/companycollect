package main

import wappalyzer "github.com/projectdiscovery/wappalyzergo"

var wapp, _ = wappalyzer.New()

// DetectTech fingerprints one page (HTTP headers + body) into a list of technologies.
// wappalyzergo returns keys as "Name" or "Name:version"; categories come from AppInfo.
func DetectTech(headers map[string][]string, body []byte) []Technology {
	out := []Technology{}
	if wapp == nil {
		return out
	}
	for key, app := range wapp.FingerprintWithInfo(headers, body) {
		name, version := key, ""
		for i := 0; i < len(key); i++ {
			if key[i] == ':' {
				name, version = key[:i], key[i+1:]
				break
			}
		}
		cat := ""
		if len(app.Categories) > 0 {
			cat = app.Categories[0]
		}
		out = append(out, Technology{Name: name, Category: cat, Version: version})
	}
	return out
}
