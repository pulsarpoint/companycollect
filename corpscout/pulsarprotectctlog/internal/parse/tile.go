package parse

import (
	"fmt"

	ct "github.com/google/certificate-transparency-go"
	"github.com/google/certificate-transparency-go/tls"
	"github.com/google/certificate-transparency-go/x509"

	"github.com/pulsarpoint/pulsarprotectctlog/internal/model"
)

// TileEntry is one parsed entry from a static-ct data tile. Consumed is the
// entry's byte length; HasMeta is false when the certificate could not be
// parsed (the leaf is skipped but the byte boundary is still known).
type TileEntry struct {
	Meta     model.CertMeta
	Consumed int
	HasMeta  bool
}

// TileLeaf parses a single static-ct-api TileLeaf from the front of data.
// A non-nil error means the TLS framing is broken and the byte boundary is
// unknown — the caller must stop parsing the tile. If the framing is intact
// but the certificate cannot be parsed, it returns HasMeta=false with a valid
// Consumed and a nil error, so the caller can skip and continue.
func TileLeaf(data []byte, startIndex int64, logName string) (TileEntry, error) {
	var te ct.TimestampedEntry
	rest, err := tls.Unmarshal(data, &te)
	if err != nil {
		return TileEntry{}, fmt.Errorf("unmarshal timestamped entry: %w", err)
	}
	consumed := len(data) - len(rest)
	if te.EntryType == ct.PrecertLogEntryType {
		if consumed, err = skipUint24Prefixed(data, consumed); err != nil {
			return TileEntry{}, fmt.Errorf("skip pre_certificate: %w", err)
		}
	}
	if consumed, err = skipUint16Prefixed(data, consumed); err != nil {
		return TileEntry{}, fmt.Errorf("skip certificate_chain: %w", err)
	}

	cert, entryType, cerr := certFromTimestampedEntry(&te)
	if cert == nil {
		// Framing is intact (we know Consumed) but the cert is unparseable:
		// skip this leaf, do not abort the tile.
		return TileEntry{Consumed: consumed, HasMeta: false}, nil
	}
	_ = cerr // non-fatal x509 issues keep the (partial) cert
	meta := BuildMeta(cert, entryType, te.Timestamp, logName, uint64(startIndex))
	return TileEntry{Meta: meta, Consumed: consumed, HasMeta: true}, nil
}

// certFromTimestampedEntry parses the leaf certificate from a TimestampedEntry.
func certFromTimestampedEntry(te *ct.TimestampedEntry) (*x509.Certificate, model.EntryType, error) {
	switch te.EntryType {
	case ct.X509LogEntryType:
		if te.X509Entry == nil {
			return nil, model.EntryTypeUnknown, fmt.Errorf("x509 entry missing certificate")
		}
		cert, err := x509.ParseCertificate(te.X509Entry.Data)
		if err != nil && cert == nil {
			return nil, model.EntryTypeUnknown, fmt.Errorf("parse x509: %w", err)
		}
		return cert, model.EntryTypeCert, nil
	case ct.PrecertLogEntryType:
		if te.PrecertEntry == nil {
			return nil, model.EntryTypeUnknown, fmt.Errorf("precert entry missing TBS")
		}
		cert, err := x509.ParseTBSCertificate(te.PrecertEntry.TBSCertificate)
		if err != nil && cert == nil {
			return nil, model.EntryTypeUnknown, fmt.Errorf("parse precert TBS: %w", err)
		}
		return cert, model.EntryTypePrecert, nil
	default:
		return nil, model.EntryTypeUnknown, fmt.Errorf("unsupported entry type %v", te.EntryType)
	}
}

// skipUint24Prefixed advances past a uint24-length-prefixed field starting at
// pos, returning the offset just after it.
func skipUint24Prefixed(data []byte, pos int) (int, error) {
	if pos+3 > len(data) {
		return 0, fmt.Errorf("truncated uint24 length at %d", pos)
	}
	n := int(data[pos])<<16 | int(data[pos+1])<<8 | int(data[pos+2])
	end := pos + 3 + n
	if end > len(data) {
		return 0, fmt.Errorf("uint24 field overruns tile (%d > %d)", end, len(data))
	}
	return end, nil
}

// skipUint16Prefixed advances past a uint16-length-prefixed field starting at
// pos, returning the offset just after it.
func skipUint16Prefixed(data []byte, pos int) (int, error) {
	if pos+2 > len(data) {
		return 0, fmt.Errorf("truncated uint16 length at %d", pos)
	}
	n := int(data[pos])<<8 | int(data[pos+1])
	end := pos + 2 + n
	if end > len(data) {
		return 0, fmt.Errorf("uint16 field overruns tile (%d > %d)", end, len(data))
	}
	return end, nil
}

// DataTile parses all entries in a static-ct data tile. parseErrors counts
// leaves that were skipped because their certificate could not be parsed. A
// non-nil err means the TLS framing broke partway; metas holds everything
// parsed before that point.
func DataTile(tile []byte, startIndex int64, logName string) (metas []model.CertMeta, parseErrors int, err error) {
	pos := 0
	idx := startIndex
	for pos < len(tile) {
		entry, perr := TileLeaf(tile[pos:], idx, logName)
		if perr != nil {
			return metas, parseErrors, fmt.Errorf("framing broken at index %d (offset %d): %w", idx, pos, perr)
		}
		if entry.Consumed <= 0 {
			return metas, parseErrors, fmt.Errorf("zero-length entry at index %d", idx)
		}
		if entry.HasMeta {
			metas = append(metas, entry.Meta)
		} else {
			parseErrors++
		}
		pos += entry.Consumed
		idx++
	}
	return metas, parseErrors, nil
}
