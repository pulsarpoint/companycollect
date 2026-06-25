package main

const (
	ccBucket     = "commoncrawl"
	techMaxBytes = 131072 // cap body fed to Wappalyzer; full-body regex over MB pages ~= 1.2s/page
)
