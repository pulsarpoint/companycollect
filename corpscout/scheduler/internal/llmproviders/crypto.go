package llmproviders

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"io"

	"github.com/cockroachdb/errors"
)

type Cipher struct {
	gcm cipher.AEAD
}

func NewCipher(base64Key string) (*Cipher, error) {
	if base64Key == "" {
		return nil, errors.New("llm provider encryption key is not configured")
	}
	key, err := base64.StdEncoding.DecodeString(base64Key)
	if err != nil {
		return nil, errors.Wrap(err, "decode llm provider encryption key")
	}
	if len(key) != 32 {
		return nil, errors.Newf("llm provider encryption key must decode to 32 bytes, got %d", len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, errors.Wrap(err, "create llm provider aes cipher")
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, errors.Wrap(err, "create llm provider gcm")
	}
	return &Cipher{gcm: gcm}, nil
}

func (c *Cipher) Encrypt(plaintext string) (EncryptedSecret, error) {
	nonce := make([]byte, c.gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return EncryptedSecret{}, errors.Wrap(err, "generate llm provider nonce")
	}
	ciphertext := c.gcm.Seal(nil, nonce, []byte(plaintext), nil)
	return EncryptedSecret{
		Ciphertext: base64.StdEncoding.EncodeToString(ciphertext),
		Nonce:      base64.StdEncoding.EncodeToString(nonce),
		Algorithm:  EncryptionAlgorithmAES256GCM,
		KeyVersion: DefaultKeyVersion,
	}, nil
}

func (c *Cipher) Decrypt(secret EncryptedSecret) (string, error) {
	if secret.Algorithm != EncryptionAlgorithmAES256GCM {
		return "", errors.Newf("unsupported llm provider key algorithm %q", secret.Algorithm)
	}
	nonce, err := base64.StdEncoding.DecodeString(secret.Nonce)
	if err != nil {
		return "", errors.Wrap(err, "decode llm provider nonce")
	}
	ciphertext, err := base64.StdEncoding.DecodeString(secret.Ciphertext)
	if err != nil {
		return "", errors.Wrap(err, "decode llm provider ciphertext")
	}
	plaintext, err := c.gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return "", errors.Wrap(err, "decrypt llm provider api key")
	}
	return string(plaintext), nil
}
