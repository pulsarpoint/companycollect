package feeds

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
)

const azurePage = "https://www.microsoft.com/en-us/download/details.aspx?id=56519"

var azureLinkRE = regexp.MustCompile(`https://download\.microsoft\.com/download/[^"'\s<>]+/ServiceTags_Public_\d{8}\.json`)

// Azure collects the weekly Service Tags file. Its URL changes every week, so
// the collector reads the download page first and follows the JSON link.
type Azure struct {
	noParams
	PageURL     string
	LinkPattern *regexp.Regexp
}

// NewAzure returns the collector for the official publication.
func NewAzure() *Azure { return &Azure{PageURL: azurePage, LinkPattern: azureLinkRE} }

// Name implements Collector.
func (*Azure) Name() string { return "azure_service_tags" }

// Collect implements Collector.
func (c *Azure) Collect(ctx context.Context, f *Fetcher, _ map[string]string) (Result, error) {
	page, err := f.Get(ctx, c.PageURL, "text/html")
	if err != nil {
		return Result{}, err
	}
	link := c.LinkPattern.Find(page.Body)
	if link == nil {
		return Result{}, fmt.Errorf("azure: ServiceTags download link not found on %s (page layout changed?)", c.PageURL)
	}
	url := string(link)
	resp, err := f.Get(ctx, url, "application/json")
	if err != nil {
		return Result{}, err
	}
	var feed struct {
		ChangeNumber int `json:"changeNumber"`
		Values       []struct {
			Name       string `json:"name"`
			Properties struct {
				Region          string   `json:"region"`
				AddressPrefixes []string `json:"addressPrefixes"`
			} `json:"properties"`
		} `json:"values"`
	}
	if err := json.Unmarshal(resp.Body, &feed); err != nil {
		return Result{}, fmt.Errorf("azure: decode: %w", err)
	}
	names := map[string]bool{}
	for _, v := range feed.Values {
		names[v.Name] = true
	}
	for _, required := range []string{"AzureCloud", "AzureFrontDoor.Frontend"} {
		if !names[required] {
			return Result{}, shapeErr("azure: service tag %q missing from %s", required, url)
		}
	}
	b := rangeBuilder{res: Result{SourceURL: url, SourceVersion: fmt.Sprintf("changeNumber=%d", feed.ChangeNumber)}}
	for _, v := range feed.Values {
		for _, p := range v.Properties.AddressPrefixes {
			b.add(p, v.Name, v.Properties.Region)
		}
	}
	return finish(b.res)
}
