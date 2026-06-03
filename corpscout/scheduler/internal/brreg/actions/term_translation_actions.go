package actions

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"

	brregdb "github.com/pulsarpoint/corpscout/scheduler/internal/brreg/db"
	"github.com/pulsarpoint/corpscout/scheduler/internal/translationclient"
)

const termTranslationPublishFlushTimeout = 5 * time.Second

type TermTranslationActions struct {
	gateway *brregdb.Gateway
	nats    *nats.Conn
}

func NewTermTranslationActions(gateway *brregdb.Gateway, natsConn *nats.Conn) *TermTranslationActions {
	return &TermTranslationActions{gateway: gateway, nats: natsConn}
}

type EnsureBrregTranslationTermsInput struct {
	TemporalWorkflowID string `json:"temporal_workflow_id"`
	Provider           string `json:"provider,omitempty"`
	Model              string `json:"model,omitempty"`
	PromptVersion      string `json:"prompt_version,omitempty"`
	Limit              int32  `json:"limit,omitempty"`
}

type EnsureBrregTranslationTermsResult struct {
	TermsInserted int32 `json:"terms_inserted"`
}

type PublishBrregTranslationTermsInput struct {
	RequestID      string `json:"request_id,omitempty"`
	Provider       string `json:"provider,omitempty"`
	Model          string `json:"model,omitempty"`
	PromptVersion  string `json:"prompt_version,omitempty"`
	Limit          int32  `json:"limit"`
	MaxAttempts    int32  `json:"max_attempts"`
	RequestSubject string `json:"request_subject,omitempty"`
}

type PublishBrregTranslationTermsResult struct {
	TermsPublished int32 `json:"terms_published"`
}

type ApplyBrregCachedTranslationTermsInput struct {
	PromptVersion string `json:"prompt_version,omitempty"`
	Limit         int32  `json:"limit,omitempty"`
}

type ApplyBrregCachedTranslationTermsResult struct {
	FieldsApplied int32 `json:"fields_applied"`
}

func termTranslationRequestFromQueuedTerms(
	input PublishBrregTranslationTermsInput,
	terms []brregdb.QueuedTranslationTerm,
) translationclient.TermTranslationRequest {
	requestID := input.RequestID
	if requestID == "" {
		requestID = uuid.NewString()
	}
	sourceLang := "no"
	targetLang := "en"
	if len(terms) > 0 {
		sourceLang = terms[0].SourceLang
		targetLang = terms[0].TargetLang
	}
	request := translationclient.TermTranslationRequest{
		RequestID:     requestID,
		Source:        "brreg",
		SourceLang:    sourceLang,
		TargetLang:    targetLang,
		Provider:      input.Provider,
		Model:         input.Model,
		PromptVersion: input.PromptVersion,
		Terms:         make([]translationclient.TermTranslationRequestTerm, 0, len(terms)),
	}
	for _, term := range terms {
		request.Terms = append(request.Terms, translationclient.TermTranslationRequestTerm{
			TermKey:              term.TermKey,
			SourceText:           term.SourceText,
			SourceTextNormalized: term.SourceTextNormalized,
		})
	}
	return request
}

func (a *TermTranslationActions) EnsureBrregTranslationTerms(
	ctx context.Context,
	input EnsureBrregTranslationTermsInput,
) (EnsureBrregTranslationTermsResult, error) {
	if a == nil || a.gateway == nil {
		return EnsureBrregTranslationTermsResult{}, errors.New("brreg term translation gateway not available")
	}
	result, err := a.gateway.EnsurePendingTranslationTerms(ctx, brregdb.EnsurePendingTranslationTermsCommand{
		Provider:      input.Provider,
		Model:         input.Model,
		PromptVersion: input.PromptVersion,
		WorkflowID:    input.TemporalWorkflowID,
		Limit:         input.Limit,
	})
	if err != nil {
		return EnsureBrregTranslationTermsResult{}, errors.Wrap(err, "ensure brreg translation terms")
	}
	return EnsureBrregTranslationTermsResult{TermsInserted: result.TermsInserted}, nil
}

func (a *TermTranslationActions) PublishBrregTranslationTerms(
	ctx context.Context,
	input PublishBrregTranslationTermsInput,
) (PublishBrregTranslationTermsResult, error) {
	if a == nil || a.gateway == nil {
		return PublishBrregTranslationTermsResult{}, errors.New("brreg term translation gateway not available")
	}
	if a.nats == nil {
		return PublishBrregTranslationTermsResult{}, errors.New("nats connection not available")
	}
	terms, err := a.gateway.ClaimQueuedTranslationTerms(ctx, brregdb.ClaimQueuedTranslationTermsCommand{
		Provider:      input.Provider,
		Model:         input.Model,
		PromptVersion: input.PromptVersion,
		Limit:         input.Limit,
		MaxAttempts:   input.MaxAttempts,
	})
	if err != nil {
		return PublishBrregTranslationTermsResult{}, errors.Wrap(err, "claim queued brreg translation terms")
	}
	if len(terms) == 0 {
		return PublishBrregTranslationTermsResult{}, nil
	}
	request := termTranslationRequestFromQueuedTerms(input, terms)
	data, err := json.Marshal(request)
	if err != nil {
		return PublishBrregTranslationTermsResult{}, errors.Wrap(err, "marshal brreg term translation request")
	}
	subject := input.RequestSubject
	if subject == "" {
		subject = translationclient.DefaultTermTranslationRequestSubject
	}
	if err := a.nats.Publish(subject, data); err != nil {
		return PublishBrregTranslationTermsResult{}, errors.Wrap(err, "publish brreg term translation request")
	}
	if err := a.nats.FlushTimeout(termTranslationPublishFlushTimeout); err != nil {
		return PublishBrregTranslationTermsResult{}, errors.Wrap(err, "flush brreg term translation request")
	}
	slog.DebugContext(ctx, "published brreg term translation request",
		"request_id", request.RequestID,
		"terms", len(terms),
		"subject", subject,
	)
	return PublishBrregTranslationTermsResult{TermsPublished: int32(len(terms))}, nil
}

func (a *TermTranslationActions) ApplyBrregCachedTranslationTerms(
	ctx context.Context,
	input ApplyBrregCachedTranslationTermsInput,
) (ApplyBrregCachedTranslationTermsResult, error) {
	if a == nil || a.gateway == nil {
		return ApplyBrregCachedTranslationTermsResult{}, errors.New("brreg term translation gateway not available")
	}
	result, err := a.gateway.ApplyCachedTermTranslations(ctx, brregdb.ApplyCachedTermTranslationsCommand{
		PromptVersion: input.PromptVersion,
		Limit:         input.Limit,
	})
	if err != nil {
		return ApplyBrregCachedTranslationTermsResult{}, errors.Wrap(err, "apply brreg cached translation terms")
	}
	return ApplyBrregCachedTranslationTermsResult{FieldsApplied: result.FieldsApplied}, nil
}
