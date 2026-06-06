import pytest
from pydantic import ValidationError

from corpscout_translation_service.models import (
    JetStreamTranslationFailureItem,
    JetStreamTranslationJob,
    JetStreamTranslationJobTerm,
    JetStreamTranslationResult,
    JetStreamTranslationResultItem,
)


TERM_KEY = "a" * 64


def test_jetstream_translation_job_accepts_scheduler_payload() -> None:
    job = JetStreamTranslationJob.model_validate(
        {
            "job_id": "job-1",
            "batch_id": "workflow/batch/000001",
            "source": "brreg",
            "source_lang": "no",
            "target_lang": "en",
            "provider": "deepseek",
            "model": "deepseek-chat",
            "prompt_version": "v1",
            "company_ids": ["company-a"],
            "terms": [
                {
                    "term_key": TERM_KEY,
                    "source_text": "Aksjeselskap",
                    "source_text_normalized": "aksjeselskap",
                }
            ],
        }
    )

    assert job.company_ids == ["company-a"]
    assert job.terms[0].term_key == TERM_KEY


def test_jetstream_translation_job_rejects_duplicate_term_keys() -> None:
    with pytest.raises(ValidationError, match="terms must not contain duplicate term_key values"):
        JetStreamTranslationJob(
            job_id="job-1",
            batch_id="workflow/batch/000001",
            source="brreg",
            source_lang="no",
            target_lang="en",
            provider="deepseek",
            model="deepseek-chat",
            prompt_version="v1",
            company_ids=["company-a"],
            terms=[
                JetStreamTranslationJobTerm(
                    term_key=TERM_KEY,
                    source_text="Aksjeselskap",
                    source_text_normalized="aksjeselskap",
                ),
                JetStreamTranslationJobTerm(
                    term_key=TERM_KEY,
                    source_text="Aksjeselskap",
                    source_text_normalized="aksjeselskap",
                ),
            ],
        )


def test_jetstream_translation_result_dump_includes_company_ids() -> None:
	result = JetStreamTranslationResult(
        job_id="job-1",
        batch_id="workflow/batch/000001",
        source="brreg",
        status="partial",
        provider="deepseek",
        model="deepseek-chat",
        prompt_version="v1",
        company_ids=["company-a"],
        duration_ms=1234,
        results=[
            JetStreamTranslationResultItem(
                term_key=TERM_KEY,
                source_text="Aksjeselskap",
                source_text_normalized="aksjeselskap",
                translated_text="Limited liability company",
                status="succeeded",
            )
        ],
        failures=[
            JetStreamTranslationFailureItem(
                term_key="b" * 64,
                source_text="Ukjent",
                source_text_normalized="ukjent",
                status="failed_terminal",
                error_code="llm_error",
                error="provider failed",
            )
		],
	)

	assert result.model_dump(mode="json") == {
		"job_id": "job-1",
		"batch_id": "workflow/batch/000001",
		"source": "brreg",
		"status": "partial",
		"provider": "deepseek",
		"model": "deepseek-chat",
		"prompt_version": "v1",
		"company_ids": ["company-a"],
		"duration_ms": 1234,
		"results": [
			{
				"term_key": TERM_KEY,
				"source_text": "Aksjeselskap",
				"source_text_normalized": "aksjeselskap",
				"translated_text": "Limited liability company",
				"status": "succeeded",
			}
		],
		"failures": [
			{
				"term_key": "b" * 64,
				"source_text": "Ukjent",
				"source_text_normalized": "ukjent",
				"status": "failed_terminal",
				"error_code": "llm_error",
				"error": "provider failed",
			}
		],
	}
