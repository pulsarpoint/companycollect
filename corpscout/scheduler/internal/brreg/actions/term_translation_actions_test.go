package actions

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	brregdb "github.com/pulsarpoint/corpscout/scheduler/internal/brreg/db"
	"github.com/pulsarpoint/corpscout/scheduler/internal/testdb"
	"github.com/pulsarpoint/corpscout/scheduler/internal/translationclient"
)

func TestTermTranslationRequestFromQueuedTerms(t *testing.T) {
	request := termTranslationRequestFromQueuedTerms(PublishBrregTranslationTermsInput{
		RequestID:     "request-1",
		Provider:      "default",
		Model:         "qwen3:6b",
		PromptVersion: "v1",
	}, []brregdb.QueuedTranslationTerm{{
		TermKey:              "abc",
		SourceLang:           "no",
		TargetLang:           "en",
		SourceText:           "Aksjeselskap",
		SourceTextNormalized: "aksjeselskap",
	}})

	require.Equal(t, "brreg", request.Source)
	require.Equal(t, "no", request.SourceLang)
	require.Equal(t, "en", request.TargetLang)
	require.Equal(t, "Aksjeselskap", request.Terms[0].SourceText)
}

func TestPublishBrregTranslationTermsPublishesDefaultSubject(t *testing.T) {
	tx := testdb.BeginTx(t)
	ctx := t.Context()
	promptVersion := "unit-test-" + uuid.NewString()
	seedPendingTranslationTerm(t, tx, promptVersion, "Aksjeselskap")

	serverURL := startTermTranslationNATSServer(t)
	subscriber := connectTermTranslationNATS(t, serverURL)
	messages := subscribeTermTranslationSubject(t, subscriber, translationclient.DefaultTermTranslationRequestSubject)
	publisher := connectTermTranslationNATS(t, serverURL)

	result, err := NewTermTranslationActions(brregdb.New(tx), publisher).PublishBrregTranslationTerms(ctx, PublishBrregTranslationTermsInput{
		RequestID:     "request-1",
		Provider:      "mock",
		Model:         "mock-fast",
		PromptVersion: promptVersion,
		Limit:         10,
		MaxAttempts:   3,
	})

	require.NoError(t, err)
	require.EqualValues(t, 1, result.TermsPublished)
	msg := receiveTermTranslationMessage(t, messages)
	require.Equal(t, translationclient.DefaultTermTranslationRequestSubject, msg.Subject)
	require.JSONEq(t, `{
	  "request_id": "request-1",
	  "source": "brreg",
	  "source_lang": "no",
	  "target_lang": "en",
	  "provider": "mock",
	  "model": "mock-fast",
	  "prompt_version": "`+promptVersion+`",
	  "terms": [
	    {
	      "term_key": "`+termTranslationTestKey("Aksjeselskap")+`",
	      "source_text": "Aksjeselskap",
	      "source_text_normalized": "aksjeselskap"
	    }
	  ]
	}`, string(msg.Data))
}

func TestPublishBrregTranslationTermsPublishesCustomSubject(t *testing.T) {
	tx := testdb.BeginTx(t)
	ctx := t.Context()
	promptVersion := "unit-test-" + uuid.NewString()
	subject := "brreg.translation.terms.request." + uuid.NewString()
	seedPendingTranslationTerm(t, tx, promptVersion, "Enhet")

	serverURL := startTermTranslationNATSServer(t)
	subscriber := connectTermTranslationNATS(t, serverURL)
	messages := subscribeTermTranslationSubject(t, subscriber, subject)
	publisher := connectTermTranslationNATS(t, serverURL)

	result, err := NewTermTranslationActions(brregdb.New(tx), publisher).PublishBrregTranslationTerms(ctx, PublishBrregTranslationTermsInput{
		RequestID:      "request-2",
		Provider:       "mock",
		Model:          "mock-fast",
		PromptVersion:  promptVersion,
		Limit:          10,
		MaxAttempts:    3,
		RequestSubject: subject,
	})

	require.NoError(t, err)
	require.EqualValues(t, 1, result.TermsPublished)
	msg := receiveTermTranslationMessage(t, messages)
	require.Equal(t, subject, msg.Subject)
	var request translationclient.TermTranslationRequest
	require.NoError(t, json.Unmarshal(msg.Data, &request))
	require.Equal(t, "request-2", request.RequestID)
	require.Equal(t, "Enhet", request.Terms[0].SourceText)
}

func TestPublishBrregTranslationTermsNoTermsDoesNotPublish(t *testing.T) {
	tx := testdb.BeginTx(t)
	ctx := t.Context()
	promptVersion := "unit-test-empty-" + uuid.NewString()
	subject := "brreg.translation.terms.request." + uuid.NewString()

	serverURL := startTermTranslationNATSServer(t)
	subscriber := connectTermTranslationNATS(t, serverURL)
	messages := subscribeTermTranslationSubject(t, subscriber, subject)
	publisher := connectTermTranslationNATS(t, serverURL)

	result, err := NewTermTranslationActions(brregdb.New(tx), publisher).PublishBrregTranslationTerms(ctx, PublishBrregTranslationTermsInput{
		RequestID:      "request-empty",
		Provider:       "mock",
		Model:          "mock-fast",
		PromptVersion:  promptVersion,
		Limit:          10,
		MaxAttempts:    3,
		RequestSubject: subject,
	})

	require.NoError(t, err)
	require.Zero(t, result.TermsPublished)
	select {
	case msg := <-messages:
		t.Fatalf("unexpected term translation message on %s: %s", msg.Subject, string(msg.Data))
	case <-time.After(100 * time.Millisecond):
	}
}

func seedPendingTranslationTerm(t *testing.T, gatewayPool brregdb.TxPool, promptVersion string, sourceText string) {
	t.Helper()
	_, err := brregdb.New(gatewayPool).UpsertTranslationTerms(t.Context(), brregdb.UpsertTranslationTermsCommand{
		Terms: []brregdb.TranslationTermResult{{
			SourceLang:           "no",
			TargetLang:           "en",
			PromptVersion:        promptVersion,
			TermKey:              termTranslationTestKey(sourceText),
			SourceTextNormalized: strings.ToLower(strings.TrimSpace(sourceText)),
			SourceText:           sourceText,
			Status:               "pending",
		}},
	})
	require.NoError(t, err)
}

func startTermTranslationNATSServer(t *testing.T) string {
	t.Helper()
	server, err := natsserver.NewServer(&natsserver.Options{
		Host:   "127.0.0.1",
		Port:   -1,
		NoLog:  true,
		NoSigs: true,
	})
	require.NoError(t, err)
	go server.Start()
	require.True(t, server.ReadyForConnections(5*time.Second))
	t.Cleanup(server.Shutdown)
	return server.ClientURL()
}

func connectTermTranslationNATS(t *testing.T, url string) *nats.Conn {
	t.Helper()
	conn, err := nats.Connect(url)
	require.NoError(t, err)
	t.Cleanup(conn.Close)
	return conn
}

func subscribeTermTranslationSubject(t *testing.T, conn *nats.Conn, subject string) chan *nats.Msg {
	t.Helper()
	messages := make(chan *nats.Msg, 1)
	_, err := conn.ChanSubscribe(subject, messages)
	require.NoError(t, err)
	require.NoError(t, conn.FlushTimeout(2*time.Second))
	return messages
}

func receiveTermTranslationMessage(t *testing.T, messages chan *nats.Msg) *nats.Msg {
	t.Helper()
	select {
	case msg := <-messages:
		return msg
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for term translation message")
		return nil
	}
}

func termTranslationTestKey(sourceText string) string {
	sum := sha256.Sum256([]byte(strings.ToLower(strings.TrimSpace(sourceText))))
	return hex.EncodeToString(sum[:])
}
