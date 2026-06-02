package nacetaxonomy

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"io"
	"path"
	"strings"

	"github.com/cockroachdb/errors"
)

type ParsedCode struct {
	Code           string
	NormalizedCode string
	Level          int16
	LevelName      string
	ParentCode     string
	Title          string
	Description    string
	Includes       string
	Excludes       string
	Notes          []byte
	SourcePayload  []byte
	SourceHash     string
}

func ParseRDFXMLNACECodes(body []byte) ([]ParsedCode, error) {
	decoder := xml.NewDecoder(bytes.NewReader(body))
	var codes []ParsedCode
	for {
		token, err := decoder.Token()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, errors.Wrap(err, "parse nace rdf xml")
		}
		start, ok := token.(xml.StartElement)
		if !ok || (start.Name.Local != "Concept" && start.Name.Local != "Description") {
			continue
		}
		code, err := parseConcept(decoder, start)
		if err != nil {
			return nil, err
		}
		if code.Code != "" && code.Title != "" {
			codes = append(codes, code)
		}
	}
	if len(codes) == 0 {
		return nil, errors.New("nace rdf xml contained no codes")
	}
	return codes, nil
}

type sourcePayload struct {
	About                 string `json:"about,omitempty"`
	IsConcept             bool   `json:"-"`
	Notation              string `json:"notation,omitempty"`
	Identifier            string `json:"identifier,omitempty"`
	PrefLabelEN           string `json:"pref_label_en,omitempty"`
	AltLabelEN            string `json:"alt_label_en,omitempty"`
	ScopeNoteEN           string `json:"scope_note_en,omitempty"`
	CoreContentNoteEN     string `json:"core_content_note_en,omitempty"`
	AdditionalContentNote string `json:"additional_content_note_en,omitempty"`
	ExclusionNoteEN       string `json:"exclusion_note_en,omitempty"`
	Broader               string `json:"broader,omitempty"`
}

func parseConcept(decoder *xml.Decoder, start xml.StartElement) (ParsedCode, error) {
	payload := sourcePayload{
		About:     attrValue(start, "about"),
		IsConcept: start.Name.Local == "Concept",
	}

	for {
		token, err := decoder.Token()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return ParsedCode{}, errors.New("parse nace rdf xml concept: unexpected end of file")
			}
			return ParsedCode{}, errors.Wrap(err, "parse nace rdf xml concept")
		}

		switch token := token.(type) {
		case xml.StartElement:
			switch token.Name.Local {
			case "type":
				if isSKOSConceptResource(attrValue(token, "resource")) {
					payload.IsConcept = true
				}
				if err := decoder.Skip(); err != nil {
					return ParsedCode{}, errors.Wrap(err, "parse nace rdf xml type")
				}
			case "notation":
				value, err := readElementText(decoder, token)
				if err != nil {
					return ParsedCode{}, err
				}
				payload.Notation = strings.TrimSpace(value)
			case "identifier":
				value, err := readElementText(decoder, token)
				if err != nil {
					return ParsedCode{}, err
				}
				identifier := strings.TrimSpace(value)
				if payload.Identifier == "" && LevelForCode(identifier) > 0 {
					payload.Identifier = identifier
				}
			case "prefLabel":
				value, err := readElementText(decoder, token)
				if err != nil {
					return ParsedCode{}, err
				}
				if isEnglishElement(token) {
					payload.PrefLabelEN = strings.TrimSpace(value)
				}
			case "altLabel":
				value, err := readElementText(decoder, token)
				if err != nil {
					return ParsedCode{}, err
				}
				if isEnglishElement(token) {
					payload.AltLabelEN = strings.TrimSpace(value)
				}
			case "scopeNote":
				value, err := readElementText(decoder, token)
				if err != nil {
					return ParsedCode{}, err
				}
				if isEnglishElement(token) {
					payload.ScopeNoteEN = strings.TrimSpace(value)
				}
			case "coreContentNote":
				value, err := readElementText(decoder, token)
				if err != nil {
					return ParsedCode{}, err
				}
				if isEnglishElement(token) {
					payload.CoreContentNoteEN = strings.TrimSpace(value)
				}
			case "additionalContentNote":
				value, err := readElementText(decoder, token)
				if err != nil {
					return ParsedCode{}, err
				}
				if isEnglishElement(token) {
					payload.AdditionalContentNote = strings.TrimSpace(value)
				}
			case "exclusionNote":
				value, err := readElementText(decoder, token)
				if err != nil {
					return ParsedCode{}, err
				}
				if isEnglishElement(token) {
					payload.ExclusionNoteEN = strings.TrimSpace(value)
				}
			case "broader":
				payload.Broader = attrValue(token, "resource")
				if err := decoder.Skip(); err != nil {
					return ParsedCode{}, errors.Wrap(err, "parse nace rdf xml broader")
				}
			default:
				if err := decoder.Skip(); err != nil {
					return ParsedCode{}, errors.Wrapf(err, "parse nace rdf xml %s", token.Name.Local)
				}
			}
		case xml.EndElement:
			if token.Name.Local == start.Name.Local {
				return parsedCodeFromPayload(payload)
			}
		}
	}
}

func parsedCodeFromPayload(payload sourcePayload) (ParsedCode, error) {
	if !payload.IsConcept {
		return ParsedCode{}, nil
	}

	code := strings.TrimSpace(payload.Notation)
	if code == "" {
		code = strings.TrimSpace(payload.Identifier)
	}
	title := strings.TrimSpace(payload.PrefLabelEN)
	if title == "" {
		title = strings.TrimSpace(payload.AltLabelEN)
	}
	if code == "" || title == "" {
		return ParsedCode{}, nil
	}

	sourcePayload, err := json.Marshal(payload)
	if err != nil {
		return ParsedCode{}, errors.Wrap(err, "marshal nace rdf source payload")
	}
	sum := sha256.Sum256(sourcePayload)

	return ParsedCode{
		Code:           code,
		NormalizedCode: NormalizeCode(code),
		Level:          LevelForCode(code),
		LevelName:      LevelNameForCode(code),
		ParentCode:     codeFromResource(payload.Broader),
		Title:          title,
		Description:    strings.TrimSpace(payload.ScopeNoteEN),
		Includes:       strings.TrimSpace(joinNotes(payload.CoreContentNoteEN, payload.AdditionalContentNote)),
		Excludes:       strings.TrimSpace(payload.ExclusionNoteEN),
		SourcePayload:  sourcePayload,
		SourceHash:     hex.EncodeToString(sum[:]),
	}, nil
}

func readElementText(decoder *xml.Decoder, start xml.StartElement) (string, error) {
	var b strings.Builder
	for {
		token, err := decoder.Token()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return "", errors.Newf("parse nace rdf xml %s: unexpected end of file", start.Name.Local)
			}
			return "", errors.Wrapf(err, "parse nace rdf xml %s", start.Name.Local)
		}

		switch token := token.(type) {
		case xml.CharData:
			b.Write(token)
		case xml.StartElement:
			if err := decoder.Skip(); err != nil {
				return "", errors.Wrapf(err, "parse nace rdf xml nested %s", token.Name.Local)
			}
		case xml.EndElement:
			if token.Name.Local == start.Name.Local {
				return b.String(), nil
			}
		}
	}
}

func attrValue(start xml.StartElement, localName string) string {
	for _, attr := range start.Attr {
		if attr.Name.Local == localName {
			return strings.TrimSpace(attr.Value)
		}
	}
	return ""
}

func isEnglishElement(start xml.StartElement) bool {
	lang := strings.ToLower(attrValue(start, "lang"))
	return lang == "en" || strings.HasPrefix(lang, "en-")
}

func isSKOSConceptResource(resource string) bool {
	return strings.TrimSpace(resource) == "http://www.w3.org/2004/02/skos/core#Concept"
}

func joinNotes(notes ...string) string {
	var parts []string
	for _, note := range notes {
		if trimmed := strings.TrimSpace(note); trimmed != "" {
			parts = append(parts, trimmed)
		}
	}
	return strings.Join(parts, "\n\n")
}

func codeFromResource(resource string) string {
	resource = strings.TrimSpace(resource)
	if resource == "" {
		return ""
	}
	return path.Base(strings.TrimRight(resource, "/"))
}
