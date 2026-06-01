from __future__ import annotations

import argparse
import json
import os

import httpx


def main() -> None:
    parser = argparse.ArgumentParser(description="Smoke test BRREG translation over HTTP.")
    parser.add_argument("--url", default=os.getenv("TRANSLATION_SERVICE_URL", "http://localhost:8095"))
    parser.add_argument("--provider", default=os.getenv("LLM_PROVIDER", "default"))
    parser.add_argument("--model", default=os.getenv("LLM_MODEL", ""))
    parser.add_argument("--base-url", default=os.getenv("LLM_BASE_URL", ""))
    parser.add_argument("--api-key-env", default="LLM_API_KEY")
    parser.add_argument("--timeout", type=float, default=180)
    args = parser.parse_args()

    payload = {
        "records": [
            {
                "record_id": "00000000-0000-0000-0000-000000000001",
                "organization_number": "810202572",
                "raw_payload": {
                    "organisasjonsnummer": "810202572",
                    "navn": "BORTIGARD AS",
                    "aktivitet": [
                        "Drive utleie av fast eiendom, maskiner og utstyr, samt kjop og salg av aksjer."
                    ],
                    "organisasjonsform": {"kode": "AS", "beskrivelse": "Aksjeselskap"},
                },
            }
        ],
        "llm": _llm_payload(args),
        "prompt_version": "v1",
        "source_lang": "no",
        "target_lang": "en",
        "max_retries": 2,
    }
    response = httpx.post(
        f"{args.url.rstrip('/')}/v1/translate/brreg-records",
        json=payload,
        timeout=args.timeout,
    )
    response.raise_for_status()
    print(json.dumps(response.json(), ensure_ascii=False, indent=2))


def _llm_payload(args: argparse.Namespace) -> dict[str, str]:
    llm = {"provider": args.provider}
    if args.model:
        llm["model"] = args.model
    if args.base_url:
        llm["base_url"] = args.base_url
    api_key = os.getenv(args.api_key_env, "")
    if api_key:
        llm["api_key"] = api_key
    return llm


if __name__ == "__main__":
    main()
