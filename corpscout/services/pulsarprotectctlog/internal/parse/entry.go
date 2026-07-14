// Package parse converts raw CT log leaf entries into storage-ready
// certificate metadata, tolerating non-fatal X.509 parse errors.
package parse

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	ct "github.com/google/certificate-transparency-go"
	"github.com/google/certificate-transparency-go/x509"

	"github.com/pulsarpoint/pulsarprotectctlog/internal/model"
)

// Entry decodes a single raw leaf entry at the given index into CertMeta.
// logName is recorded on the result. Non-fatal X.509 parse errors are
// tolerated; only fatal decode failures return an error.
func Entry(index int64, leaf *ct.LeafEntry, logName string) (model.CertMeta, error) {
	raw, err := ct.RawLogEntryFromLeaf(index, leaf)
	if err != nil {
		return model.CertMeta{}, fmt.Errorf("decode leaf %d: %w", index, err)
	}
	if raw.Leaf.TimestampedEntry == nil {
		return model.CertMeta{}, fmt.Errorf("entry %d missing timestamped entry", index)
	}

	logEntry, err := raw.ToLogEntry()
	if err != nil && (logEntry == nil || (logEntry.X509Cert == nil && logEntry.Precert == nil)) {
		return model.CertMeta{}, fmt.Errorf("parse entry %d: %w", index, err)
	}

	var cert *x509.Certificate
	entryType := model.EntryTypeUnknown
	switch {
	case logEntry.X509Cert != nil:
		cert = logEntry.X509Cert
		entryType = model.EntryTypeCert
	case logEntry.Precert != nil:
		cert = logEntry.Precert.TBSCertificate
		entryType = model.EntryTypePrecert
	default:
		return model.CertMeta{}, fmt.Errorf("entry %d has no certificate", index)
	}

	return BuildMeta(cert, entryType, raw.Leaf.TimestampedEntry.Timestamp, logName, uint64(index)), nil
}

// BuildMeta assembles CertMeta from a parsed certificate and the log-entry
// envelope fields. Shared by the RFC 6962 and static-ct (tile) parsers.
func BuildMeta(cert *x509.Certificate, entryType model.EntryType, tsMillis uint64, logName string, index uint64) model.CertMeta {
	return model.CertMeta{
		IssuerCAID:         hashBytes(cert.RawIssuer),
		IssuerName:         cert.Issuer.String(),
		SerialNumber:       serialHex(cert),
		FingerprintSHA256:  hashBytes(cert.Raw),
		CommonName:         cert.Subject.CommonName,
		SANs:               cert.DNSNames,
		NotBefore:          cert.NotBefore.UTC(),
		NotAfter:           cert.NotAfter.UTC(),
		SCTTimestamp:       time.UnixMilli(int64(tsMillis)).UTC(),
		LogName:            logName,
		LogIndex:           index,
		EntryType:          entryType,
		SignatureAlgorithm: cert.SignatureAlgorithm.String(),
		PublicKeyAlgorithm: cert.PublicKeyAlgorithm.String(),
		KeySize:            keySize(cert),
		IsCA:               cert.IsCA,
		IsWildcard:         isWildcard(cert.Subject.CommonName, cert.DNSNames),
	}
}

func hashBytes(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func serialHex(cert *x509.Certificate) string {
	if cert.SerialNumber == nil {
		return ""
	}
	return hex.EncodeToString(cert.SerialNumber.Bytes())
}

func keySize(cert *x509.Certificate) int {
	switch pub := cert.PublicKey.(type) {
	case *rsa.PublicKey:
		return pub.N.BitLen()
	case *ecdsa.PublicKey:
		if pub.Curve != nil {
			return pub.Curve.Params().BitSize
		}
	case ed25519.PublicKey:
		return 256
	}
	return 0
}

func isWildcard(commonName string, sans []string) bool {
	if strings.HasPrefix(commonName, "*.") {
		return true
	}
	for _, s := range sans {
		if strings.HasPrefix(s, "*.") {
			return true
		}
	}
	return false
}
