package cache

import (
	"context"
	"sync"
	"time"

	"github.com/pulsarpoint/currency-service/internal/rates"
	"golang.org/x/sync/singleflight"
)

type entry struct {
	sheet     *rates.RateSheet
	expiresAt time.Time // zero means never expires
}

// Cache is a process-local in-memory store for rate sheets.
// today=true in Get means the entry uses the configured todayTTL;
// today=false means the entry is cached for the process lifetime.
type Cache struct {
	mu       sync.RWMutex
	entries  map[string]*entry
	todayTTL time.Duration
	group    singleflight.Group
}

// New creates a Cache with the given TTL for "today" entries.
func New(todayTTL time.Duration) *Cache {
	return &Cache{
		entries:  make(map[string]*entry),
		todayTTL: todayTTL,
	}
}

// Get returns a cached RateSheet or calls fetch. today=true applies the TTL.
// Returns (sheet, cacheHit, error).
func (c *Cache) Get(
	ctx context.Context,
	key string,
	today bool,
	fetch func(context.Context) (*rates.RateSheet, error),
) (*rates.RateSheet, bool, error) {
	c.mu.RLock()
	e, ok := c.entries[key]
	c.mu.RUnlock()

	if ok && (e.expiresAt.IsZero() || time.Now().Before(e.expiresAt)) {
		return e.sheet, true, nil
	}

	type result struct {
		sheet *rates.RateSheet
	}
	v, err, _ := c.group.Do(key, func() (interface{}, error) {
		sheet, err := fetch(ctx)
		if err != nil {
			return nil, err
		}
		ent := &entry{sheet: sheet}
		if today {
			ent.expiresAt = time.Now().Add(c.todayTTL)
		}
		c.mu.Lock()
		c.entries[key] = ent
		c.mu.Unlock()
		return &result{sheet: sheet}, nil
	})
	if err != nil {
		return nil, false, err
	}
	return v.(*result).sheet, false, nil
}
