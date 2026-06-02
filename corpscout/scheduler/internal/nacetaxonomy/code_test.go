package nacetaxonomy

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizeCode(t *testing.T) {
	require.Equal(t, "6820", NormalizeCode("68.20"))
	require.Equal(t, "682", NormalizeCode("68.2"))
	require.Equal(t, "L", NormalizeCode(" l "))
}

func TestLevelNameForCode(t *testing.T) {
	require.Equal(t, "section", LevelNameForCode("L"))
	require.Equal(t, "division", LevelNameForCode("68"))
	require.Equal(t, "group", LevelNameForCode("68.2"))
	require.Equal(t, "class", LevelNameForCode("68.20"))
}

func TestLevelForCode(t *testing.T) {
	require.Equal(t, int16(1), LevelForCode("L"))
	require.Equal(t, int16(2), LevelForCode("68"))
	require.Equal(t, int16(3), LevelForCode("68.2"))
	require.Equal(t, int16(4), LevelForCode("68.20"))
}

func TestNACEClassFromNorwegianSNCode(t *testing.T) {
	require.Equal(t, "68.20", ClassFromNorwegianSNCode("68.200"))
	require.Equal(t, "01.11", ClassFromNorwegianSNCode("01.110"))
	require.Empty(t, ClassFromNorwegianSNCode("L"))
	require.Empty(t, ClassFromNorwegianSNCode("68.20"))
}
