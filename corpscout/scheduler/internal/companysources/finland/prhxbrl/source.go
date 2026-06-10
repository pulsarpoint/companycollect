package prhxbrl

import "github.com/pulsarpoint/corpscout/scheduler/internal/companysources"

type Source struct{}

func (Source) Key() companysources.Key {
	return companysources.Key{Country: "finland", Source: SourceKey}
}

func (Source) DisplayName() string {
	return "Finland PRH financial XBRL"
}
