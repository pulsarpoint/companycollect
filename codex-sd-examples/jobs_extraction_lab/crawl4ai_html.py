"""Extract complete pages from Crawl4AI's native HTML or Markdown, without selectors."""

import asyncio
import json
import re
import time
from importlib.metadata import version
from pathlib import Path
from typing import Any

import click
import httpx
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig
from dotenv import dotenv_values
from pydantic import ValidationError

from jobs_extraction_lab.corpus import content_hash, load_pages, utc_now, write_json
from jobs_extraction_lab.extract import extract_openrouter
from jobs_extraction_lab.format_benchmark import FORMAT_INSTRUCTIONS, format_prompt
from jobs_extraction_lab.models import JobExtraction


async def prepare_html(source_dir: Path, output_dir: Path) -> dict[str, Any]:
    """Replay saved rendered pages through the same processing used after a crawl."""
    if output_dir.exists():
        raise ValueError("Use a new output directory to preserve frozen inputs")
    pages = load_pages(source_dir)
    if not pages:
        raise ValueError("No saved pages")
    config = CrawlerRunConfig(verbose=False)
    crawler = AsyncWebCrawler(config=BrowserConfig(verbose=False))
    inputs = []
    files = {}
    for page in pages:
        html = (source_dir / page.html_file).read_text(encoding="utf-8")
        # This invokes Crawl4AI itself, without starting a browser or fetching URLs.
        result = await crawler.aprocess_html(
            url=page.final_url,
            html=html,
            extracted_content="",
            config=config,
            screenshot_data="",
            pdf_data="",
            verbose=False,
        )
        if not result.success or not result.cleaned_html:
            raise ValueError(f"Crawl4AI produced no cleaned HTML: {page.id}")
        filename = f"inputs/{page.id}.html"
        files[filename] = result.cleaned_html
        if result.markdown is None:
            raise ValueError(f"Crawl4AI produced no Markdown: {page.id}")
        markdown_file = f"native-markdown/{page.id}.md"
        files[markdown_file] = result.markdown.raw_markdown
        inputs.append(
            {
                "page_id": page.id,
                "source_url": page.final_url,
                "source_html_file": page.html_file,
                "source_html_sha256": content_hash(html),
                "source_chars": len(html),
                "format": "crawl4ai_cleaned_html",
                "file": filename,
                "sha256": content_hash(result.cleaned_html),
                "chars": len(result.cleaned_html),
                "bytes": len(result.cleaned_html.encode("utf-8")),
                "markdown_file": markdown_file,
                "markdown_sha256": content_hash(result.markdown.raw_markdown),
                "markdown_chars": len(result.markdown.raw_markdown),
                "markdown_bytes": len(result.markdown.raw_markdown.encode("utf-8")),
            }
        )
    manifest = {
        "created_at": utc_now(),
        "source_dir": str(source_dir.resolve()),
        "crawl4ai_version": version("crawl4ai"),
        "crawler_run_config": config.dump(),
        "policy": "Complete default result.cleaned_html, copied unchanged. No selectors, card reconstruction, known job URLs, title labels, or reference outputs enter preprocessing or inference. Saved rendered HTML is replayed without a fresh network crawl.",
        "inputs": inputs,
    }
    for filename, content in files.items():
        path = output_dir / filename
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
    write_json(output_dir / "manifest.json", manifest)
    return manifest


async def run_html(
    data_dir: Path,
    *,
    run_id: str,
    env_file: Path,
    examples_file: Path,
    model: str,
    provider: str,
    max_tokens: int,
    timeout: int,
    attempts: int,
    interval: float,
    input_format: str,
) -> None:
    manifest_text = (data_dir / "manifest.json").read_text(encoding="utf-8")
    manifest = json.loads(manifest_text)
    examples = examples_file.read_text(encoding="utf-8")
    api_key = dotenv_values(env_file).get("OPENROUTER_API_KEY")
    if not api_key:
        raise ValueError("OPENROUTER_API_KEY is missing")
    settings = {
        "manifest_sha256": content_hash(manifest_text),
        "model": model,
        "provider": {
            "only": [provider],
            "allow_fallbacks": False,
            "require_parameters": True,
        },
        "reasoning": {"enabled": True, "exclude": True, "effort": "low"},
        "max_tokens": max_tokens,
        "temperature": 0,
        "timeout": timeout,
        "attempts": attempts,
        "interval": interval,
        "instructions": FORMAT_INSTRUCTIONS,
        "examples": examples,
        "schema": JobExtraction.model_json_schema(),
        "input_policy": "One complete Crawl4AI cleaned HTML page per call; no truncation or segmentation",
    }
    if input_format == "markdown":
        settings["input_policy"] = (
            "One complete Crawl4AI native Markdown page per call; no truncation or segmentation"
        )
    root = data_dir / "runs" / run_id
    settings_path = root / "settings.json"
    if (
        settings_path.exists()
        and json.loads(settings_path.read_text(encoding="utf-8")) != settings
    ):
        raise ValueError("Settings changed; use a new run ID")
    write_json(settings_path, settings)
    service_errors = 0
    async with httpx.AsyncClient(timeout=httpx.Timeout(timeout, connect=20)) as client:
        for prepared in manifest["inputs"]:
            item = prepared
            if input_format == "markdown":
                item = {
                    **prepared,
                    "format": "crawl4ai_native_markdown",
                    "file": prepared["markdown_file"],
                    "sha256": prepared["markdown_sha256"],
                }
            content = (data_dir / item["file"]).read_text(encoding="utf-8")
            if content_hash(content) != item["sha256"]:
                raise ValueError(f"Input changed: {item['file']}")
            prompt = format_prompt(item, content, examples)
            input_hash = content_hash(prompt + json.dumps(settings, sort_keys=True))
            path = root / "responses" / f"{item['page_id']}.json"
            if path.exists():
                cached = json.loads(path.read_text(encoding="utf-8"))
                if cached["input_hash"] != input_hash:
                    raise ValueError(f"Cached input changed: {path}")
                continue
            started_at = utc_now()
            started = time.monotonic()
            payload = await extract_openrouter(
                client,
                prompt,
                api_key=api_key,
                model=model,
                reasoning=settings["reasoning"],
                provider_options=settings["provider"],
                timeout=timeout,
                attempts=attempts,
                max_tokens=max_tokens,
            )
            error = str(payload["error"]) if "error" in payload else None
            choice = (payload.get("choices") or [{}])[0]
            text = choice.get("message", {}).get("content")
            extraction = None
            if error is None:
                if choice.get("finish_reason") != "stop":
                    error = f"Incomplete response: finish_reason={choice.get('finish_reason')}"
                elif not isinstance(text, str):
                    error = "No text content"
                else:
                    try:
                        extraction = JobExtraction.model_validate_json(text)
                    except ValidationError as failure:
                        error = f"Invalid extraction: {failure}"
            record = {
                "page_id": item["page_id"],
                "input_hash": input_hash,
                "content_sha256": item["sha256"],
                "requested_model": model,
                "actual_model": payload.get("model"),
                "provider": payload.get("provider"),
                "response_id": payload.get("id"),
                "started_at": started_at,
                "elapsed_seconds": round(time.monotonic() - started, 3),
                "succeeded": extraction is not None and error is None,
                "extraction": extraction.model_dump()
                if extraction is not None
                else None,
                "raw_response": text,
                "usage": payload.get("usage"),
                "finish_reason": choice.get("finish_reason"),
                "attempts": payload.get("attempts", 1),
                "error": error,
            }
            write_json(path, record)
            click.echo(
                f"{item['page_id']}: {len(extraction.jobs) if extraction else 'FAILED'} jobs; {record['elapsed_seconds']}s"
            )
            permanent_error = error is not None and any(
                f"HTTP {s}" in error for s in (400, 401, 402, 403, 404, 422)
            )
            service_error = error is not None and (
                "total time limit" in error
                or any(f"HTTP {s}" in error for s in (429, 500, 502, 503, 504))
            )
            service_errors = service_errors + 1 if service_error else 0
            if permanent_error or service_errors >= 3:
                write_json(
                    root / "stopped.json", {"created_at": utc_now(), "reason": error}
                )
                click.echo(
                    "Stopped after request/routing failure or three consecutive service errors"
                )
                return
            await asyncio.sleep(interval)


@click.group()
def main() -> None:
    """Prepare native Crawl4AI outputs and run DeepSeek on entire pages."""


@main.command("prepare")
@click.option(
    "--source-dir", type=click.Path(path_type=Path, exists=True), required=True
)
@click.option("--output-dir", type=click.Path(path_type=Path), required=True)
def prepare(source_dir: Path, output_dir: Path) -> None:
    manifest = asyncio.run(prepare_html(source_dir, output_dir))
    click.echo(f"Prepared HTML and Markdown for {len(manifest['inputs'])} full pages")


@main.command("run")
@click.option("--data-dir", type=click.Path(path_type=Path, exists=True), required=True)
@click.option("--run-id", required=True)
@click.option("--env-file", type=click.Path(path_type=Path, exists=True), required=True)
@click.option(
    "--examples-file",
    type=click.Path(path_type=Path, exists=True),
    default=Path(__file__).parent / "prompt_examples.md",
)
@click.option("--model", default="deepseek/deepseek-v4-flash-0731", show_default=True)
@click.option("--provider", default="baidu/fp8", show_default=True)
@click.option("--max-tokens", type=click.IntRange(min=1), default=32768)
@click.option("--timeout", type=click.IntRange(min=1), default=180)
@click.option("--attempts", type=click.IntRange(1, 5), default=2)
@click.option("--interval", type=click.FloatRange(min=0), default=3.0)
@click.option(
    "--input-format",
    type=click.Choice(["html", "markdown"]),
    default="html",
    show_default=True,
)
def run(
    data_dir: Path,
    run_id: str,
    env_file: Path,
    examples_file: Path,
    model: str,
    provider: str,
    max_tokens: int,
    timeout: int,
    attempts: int,
    interval: float,
    input_format: str,
) -> None:
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", run_id) is None:
        raise click.ClickException("Invalid run ID")
    asyncio.run(
        run_html(
            data_dir,
            run_id=run_id,
            env_file=env_file,
            examples_file=examples_file,
            model=model,
            provider=provider,
            max_tokens=max_tokens,
            timeout=timeout,
            attempts=attempts,
            interval=interval,
            input_format=input_format,
        )
    )


if __name__ == "__main__":
    main()
