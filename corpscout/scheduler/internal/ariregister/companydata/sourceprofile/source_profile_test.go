package sourceprofile

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestBuildBatchMapsGeneralDataPayload(t *testing.T) {
	rawID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	payload := json.RawMessage(`{
		"ariregistri_kood": 16752073,
		"nimi": "007 Agent & Partners OÜ",
		"yldandmed": {
			"esmaregistreerimise_kpv": "05.06.2023",
			"staatus": "R",
			"staatus_tekstina": "Registrisse kantud",
			"piirkond": 5,
			"piirkond_tekstina": "Tartu",
			"piirkond_tekstina_pikk": "Tartu Maakohtu registriosakond",
			"oiguslik_vorm": "OÜ",
			"oiguslik_vorm_nr": 5,
			"oiguslik_vorm_tekstina": "Osaühing",
			"tegutseb_tekstina": "Jah",
			"aadressid": [{"kirje_id": 1, "riik": "EST", "riik_tekstina": "Eesti", "ehak": "0596", "ehak_nimetus": "Pirita linnaosa, Tallinn, Harju maakond", "tanav_maja_korter": "Regati pst 12", "postiindeks": "11911", "aadress_ads__ads_normaliseeritud_taisaadress": "Harju maakond, Tallinn, Pirita linnaosa, Regati pst 12"}],
			"sidevahendid": [{"kirje_id": 2, "liik": "EMAIL", "liik_tekstina": "Elektronposti aadress", "sisu": "info@example.ee"}, {"kirje_id": 3, "liik": "WWW", "liik_tekstina": "Interneti WWW aadress", "sisu": "https://example.ee/"}],
			"teatatud_tegevusalad": [{"kirje_id": 4, "emtak_kood": "73111", "emtak_tekstina": "Reklaamiagentuuride tegevus", "emtak_versioon": 3, "emtak_versioon_tekstina": "EMTAK 2025", "nace_kood": "73.11", "on_pohitegevusala": true}],
			"kapitalid": [{"kirje_id": 5, "kapitali_suurus": "0.01", "kapitali_valuuta": "EUR", "kapitali_valuuta_tekstina": "euro"}],
			"info_majandusaasta_aruannetest": [{"kirje_id": 6, "majandusaasta_perioodi_algus_kpv": "01.01.2024", "majandusaasta_perioodi_lopp_kpv": "31.12.2024", "tootajate_arv": "0", "tegevusala_emtak_kood": "73111", "tegevusala_emtak_tekstina": "Reklaamiagentuuride tegevus"}]
		}
	}`)

	batch, err := BuildBatch(Command{Trigger: "manual", Records: []RawRecord{{
		ID: rawID, RegistryCode: "16752073", LegalName: "007 Agent & Partners OÜ", CountryISO2: "EE", RawPayload: payload, PayloadHash: "hash",
	}}})

	require.NoError(t, err)
	require.Len(t, batch.Companies, 1)
	require.Equal(t, "16752073", batch.Companies[0].RegistryCode)
	require.Equal(t, "007 Agent & Partners OÜ", batch.Companies[0].LegalName)
	require.Equal(t, "Registrisse kantud", batch.Companies[0].RegistrationStatusLabel)
	require.Equal(t, "Osaühing", batch.Companies[0].LegalFormLabel)
	require.Len(t, batch.Addresses, 1)
	require.Equal(t, "Pirita linnaosa, Tallinn, Harju maakond", batch.Addresses[0].EHAKName)
	require.Len(t, batch.Contacts, 2)
	require.Len(t, batch.Websites, 1)
	require.Len(t, batch.Domains, 1)
	require.Len(t, batch.Industries, 1)
	require.True(t, batch.Industries[0].IsPrimary)
	require.Len(t, batch.Capital, 1)
	require.Len(t, batch.AnnualReports, 1)
}
