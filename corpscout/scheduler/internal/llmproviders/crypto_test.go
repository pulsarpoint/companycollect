package llmproviders

import (
	"testing"

	"github.com/stretchr/testify/require"
)

const testBase64Key = "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY="

func TestEncryptDecryptAPIKeyRoundTrip(t *testing.T) {
	cipher, err := NewCipher(testBase64Key)
	require.NoError(t, err)

	secret, err := cipher.Encrypt("secret-value")
	require.NoError(t, err)
	require.Equal(t, EncryptionAlgorithmAES256GCM, secret.Algorithm)
	require.Equal(t, DefaultKeyVersion, secret.KeyVersion)
	require.NotEmpty(t, secret.Ciphertext)
	require.NotEmpty(t, secret.Nonce)

	plaintext, err := cipher.Decrypt(secret)
	require.NoError(t, err)
	require.Equal(t, "secret-value", plaintext)
}

func TestNewCipherRejectsEmptyKey(t *testing.T) {
	cipher, err := NewCipher("")

	require.Nil(t, cipher)
	require.ErrorContains(t, err, "not configured")
}

func TestNewCipherAcceptsRawPassphrase(t *testing.T) {
	cipher, err := NewCipher("not-base64")

	require.NoError(t, err)
	secret, err := cipher.Encrypt("secret-value")
	require.NoError(t, err)
	plaintext, err := cipher.Decrypt(secret)
	require.NoError(t, err)
	require.Equal(t, "secret-value", plaintext)
}

func TestNewCipherRejectsExplicitBase64WrongLengthKey(t *testing.T) {
	cipher, err := NewCipher("base64:c2hvcnQ=")

	require.Nil(t, cipher)
	require.ErrorContains(t, err, "32 bytes")
}

func TestDecryptRejectsInvalidNonce(t *testing.T) {
	cipher, err := NewCipher(testBase64Key)
	require.NoError(t, err)

	_, err = cipher.Decrypt(EncryptedSecret{
		Ciphertext: "Y2lwaGVydGV4dA==",
		Nonce:      "not-base64",
		Algorithm:  EncryptionAlgorithmAES256GCM,
		KeyVersion: DefaultKeyVersion,
	})

	require.ErrorContains(t, err, "decode llm provider nonce")
}
