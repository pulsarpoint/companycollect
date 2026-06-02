package brregdb

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestMapRawRecordIndustriesToNACERequiresRawRecordID(t *testing.T) {
	gateway := New(nil)

	_, err := gateway.MapRawRecordIndustriesToNACE(t.Context(), MapRawRecordIndustriesToNACECommand{})

	require.ErrorContains(t, err, "raw record id is required")
}

func TestMapRawRecordIndustriesToNACERequiresDatabase(t *testing.T) {
	gateway := New(nil)

	_, err := gateway.MapRawRecordIndustriesToNACE(t.Context(), MapRawRecordIndustriesToNACECommand{
		RawRecordID: uuid.New(),
	})

	require.ErrorContains(t, err, "brreg workflow database pool not available")
}

func TestListRawRecordNACEMappingsRequiresRawRecordID(t *testing.T) {
	gateway := New(nil)

	_, err := gateway.ListRawRecordNACEMappings(t.Context(), uuid.Nil)

	require.ErrorContains(t, err, "raw record id is required")
}
