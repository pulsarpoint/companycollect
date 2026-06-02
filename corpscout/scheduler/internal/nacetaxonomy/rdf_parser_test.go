package nacetaxonomy

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseRDFXMLNACECodes(t *testing.T) {
	body := []byte(`<?xml version="1.0" encoding="UTF-8"?>
<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
         xmlns:skos="http://www.w3.org/2004/02/skos/core#">
  <skos:Concept rdf:about="http://data.europa.eu/ux2/nace2.1/M">
    <skos:notation>M</skos:notation>
    <skos:prefLabel xml:lang="en">Real estate activities</skos:prefLabel>
    <skos:prefLabel xml:lang="fr">Activites immobilieres</skos:prefLabel>
  </skos:Concept>
  <skos:Concept rdf:about="http://data.europa.eu/ux2/nace2.1/68">
    <skos:notation>68</skos:notation>
    <skos:broader rdf:resource="http://data.europa.eu/ux2/nace2.1/M"/>
    <skos:prefLabel xml:lang="en">Real estate activities</skos:prefLabel>
  </skos:Concept>
  <skos:Concept rdf:about="http://data.europa.eu/ux2/nace2.1/68.20">
    <skos:notation>68.20</skos:notation>
    <skos:broader rdf:resource="http://data.europa.eu/ux2/nace2.1/68.2"/>
    <skos:prefLabel xml:lang="en">Renting and operating of own or leased real estate</skos:prefLabel>
    <skos:scopeNote xml:lang="en">This class includes landlords.</skos:scopeNote>
  </skos:Concept>
</rdf:RDF>`)

	codes, err := ParseRDFXMLNACECodes(body)

	require.NoError(t, err)
	require.Len(t, codes, 3)
	require.Equal(t, "M", codes[0].Code)
	require.Equal(t, "M", codes[0].NormalizedCode)
	require.Equal(t, int16(1), codes[0].Level)
	require.Equal(t, "section", codes[0].LevelName)
	require.Empty(t, codes[0].ParentCode)
	require.Equal(t, "Real estate activities", codes[0].Title)
	require.Equal(t, "68", codes[1].Code)
	require.Equal(t, "M", codes[1].ParentCode)
	require.Equal(t, "68.20", codes[2].Code)
	require.Equal(t, "6820", codes[2].NormalizedCode)
	require.Equal(t, "class", codes[2].LevelName)
	require.Equal(t, "68.2", codes[2].ParentCode)
	require.Contains(t, codes[2].Description, "landlords")
	require.NotEmpty(t, codes[2].SourcePayload)
	require.NotEmpty(t, codes[2].SourceHash)
}

func TestParseRDFXMLNACECodesBuildsSourcePayload(t *testing.T) {
	body := []byte(`<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:skos="http://www.w3.org/2004/02/skos/core#">
  <skos:Concept rdf:about="http://example.test/nace/01.11">
    <skos:notation>01.11</skos:notation>
    <skos:broader rdf:resource="http://example.test/nace/01.1"/>
    <skos:prefLabel xml:lang="en">Growing of cereals</skos:prefLabel>
    <skos:scopeNote xml:lang="en">Includes seed production.</skos:scopeNote>
  </skos:Concept>
</rdf:RDF>`)

	codes, err := ParseRDFXMLNACECodes(body)

	require.NoError(t, err)
	require.Len(t, codes, 1)
	require.JSONEq(t, `{
		"about": "http://example.test/nace/01.11",
		"notation": "01.11",
		"pref_label_en": "Growing of cereals",
		"scope_note_en": "Includes seed production.",
		"broader": "http://example.test/nace/01.1"
	}`, string(codes[0].SourcePayload))

	var payload map[string]string
	require.NoError(t, json.Unmarshal(codes[0].SourcePayload, &payload))
	require.Equal(t, "01.1", codes[0].ParentCode)
}

func TestParseRDFXMLNACECodesParsesOfficialSKOSCoreDescriptionShape(t *testing.T) {
	body := []byte(`<?xml version="1.0" encoding="UTF-8"?>
<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">
  <rdf:Description rdf:about="http://data.europa.eu/ux2/nace2.1/0111">
    <rdf:type rdf:resource="http://www.w3.org/2004/02/skos/core#Concept"/>
    <identifier xmlns="http://purl.org/dc/elements/1.1/">0111</identifier>
    <identifier xmlns="http://purl.org/dc/terms/" rdf:datatype="http://publications.europa.eu/resource/authority/notation-type/SDMX">C0111</identifier>
    <coreContentNote xmlns="http://rdf-vocabulary.ddialliance.org/xkos#" xml:lang="en">This class includes cereals.</coreContentNote>
    <exclusionNote xmlns="http://rdf-vocabulary.ddialliance.org/xkos#" xml:lang="en">This class excludes seed processing.</exclusionNote>
    <altLabel xmlns="http://www.w3.org/2004/02/skos/core#" xml:lang="en">Growing of cereals other than rice</altLabel>
    <broader xmlns="http://www.w3.org/2004/02/skos/core#" rdf:resource="http://data.europa.eu/ux2/nace2.1/011"/>
  </rdf:Description>
  <rdf:Description rdf:about="http://data.europa.eu/ux2/nace2.1/not-a-concept">
    <identifier xmlns="http://purl.org/dc/elements/1.1/">9999</identifier>
    <altLabel xmlns="http://www.w3.org/2004/02/skos/core#" xml:lang="en">Not a concept</altLabel>
  </rdf:Description>
</rdf:RDF>`)

	codes, err := ParseRDFXMLNACECodes(body)

	require.NoError(t, err)
	require.Len(t, codes, 1)
	require.Equal(t, "0111", codes[0].Code)
	require.Equal(t, "0111", codes[0].NormalizedCode)
	require.Equal(t, int16(4), codes[0].Level)
	require.Equal(t, "class", codes[0].LevelName)
	require.Equal(t, "011", codes[0].ParentCode)
	require.Equal(t, "Growing of cereals other than rice", codes[0].Title)
	require.Contains(t, codes[0].Includes, "cereals")
	require.Contains(t, codes[0].Excludes, "seed processing")
}

func TestParseRDFXMLNACECodesParsesDownloadedFixtureWhenProvided(t *testing.T) {
	path := os.Getenv("NACE_RDF_FIXTURE_PATH")
	if path == "" {
		t.Skip("set NACE_RDF_FIXTURE_PATH to validate a downloaded NACE RDF file")
	}
	body, err := os.ReadFile(path)
	require.NoError(t, err)

	codes, err := ParseRDFXMLNACECodes(body)

	require.NoError(t, err)
	require.Greater(t, len(codes), 900)
	require.Contains(t, codeTitles(codes), "A")
	require.Contains(t, codeTitles(codes), "01.11")
}

func TestParseRDFXMLNACECodesSkipsConceptsWithoutEnglishLabel(t *testing.T) {
	body := []byte(`<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:skos="http://www.w3.org/2004/02/skos/core#">
  <skos:Concept rdf:about="http://example.test/nace/99">
    <skos:notation>99</skos:notation>
    <skos:prefLabel xml:lang="fr">Activites inconnues</skos:prefLabel>
  </skos:Concept>
</rdf:RDF>`)

	_, err := ParseRDFXMLNACECodes(body)

	require.ErrorContains(t, err, "nace rdf xml contained no codes")
}

func TestParseRDFXMLNACECodesRejectsMalformedXML(t *testing.T) {
	_, err := ParseRDFXMLNACECodes([]byte(`<rdf:RDF><skos:Concept>`))

	require.ErrorContains(t, err, "parse nace rdf xml")
}

func codeTitles(codes []ParsedCode) map[string]string {
	titles := make(map[string]string, len(codes))
	for _, code := range codes {
		titles[code.Code] = code.Title
	}
	return titles
}
