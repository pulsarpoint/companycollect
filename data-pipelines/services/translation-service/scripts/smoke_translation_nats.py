from __future__ import annotations

import argparse
import asyncio
import json
import os

import nats


async def main() -> None:
    parser = argparse.ArgumentParser(description="Smoke test BRREG translation over NATS request/reply.")
    parser.add_argument("--nats-url", default=os.getenv("NATS_URL", "nats://localhost:4222"))
    parser.add_argument("--subject", default=os.getenv("TRANSLATION_NATS_SUBJECT", "brreg.translation.translate"))
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
    nc = await nats.connect(args.nats_url)
    try:
        response = await nc.request(args.subject, json.dumps(payload).encode("utf-8"), timeout=args.timeout)
    finally:
        await nc.drain()
    print(json.dumps(json.loads(response.data.decode("utf-8")), ensure_ascii=False, indent=2))


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
    asyncio.run(main())
