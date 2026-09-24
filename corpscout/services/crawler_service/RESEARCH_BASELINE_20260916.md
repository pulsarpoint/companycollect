# Saved crawler-service baseline

16 September 2026. The completed research is preserved before implementing the
[DSPy RLM experiment](DSPY_RLM_PLAN.md). Main package version: **0.15.2**.

## Git checkpoint

- Repository: `/Users/graovic/pulsarpoint/ppoint/companycollect`.
- Baseline branch: `checkpoint/crawler-service-20260916`.
- Experiment branch, starting at the same checkpoint: `experiment/crawler-service-dspy-rlm`.
- Scope: code, tests, benchmark harnesses, reports and research notes under
  `codex-sd-examples/`. Other working-tree changes are preserved separately and are
  not included in this checkpoint. Nothing has been pushed.

The experiment branch initially contains the plan and unchanged baseline runtime.
DSPy has not been installed and no RLM model call or new crawl has been run.
The baseline branch remains available when the experiment branch later changes.

## Local source and result archive

- Archive: `/Users/graovic/pulsarpoint/crawler-service-snapshots/20260916T134349Z/research-files.tar.gz`.
- Manifest: `/Users/graovic/pulsarpoint/crawler-service-snapshots/20260916T134349Z/manifest.json`.
- Files: **12,195**, totaling **933,872,158 bytes** before compression.
- Archive size: **254,860,497 bytes**.
- Archive SHA-256: `c92c9604688101757e60052a00ee22b88f1ae7fe80cb85102e2c7374180f1366`.

The archive includes regular research files from the package and all its earlier
labs: saved HTML/Markdown/documents, model requests and responses, catalog
snapshots, experiments, reports and code. Original files remain in their existing
directories. Ignored research data is included in this archive, not forced into Git.
This is a local backup on the same machine, not a remote/off-machine copy.

Credentials, installed Python/Node environments, caches, build outputs and symlinks
are excluded. The manifest lists every included path with its byte count and
SHA-256, plus excluded paths. Every archive member was read back and hash-checked.
All 12,195 included files were scanned for known API-key values and common API-key
patterns; no matches were found. This receipt was created after the archive and is
saved in Git and beside the archive as a separate receipt.

To recover files, extract the archive into a **new directory** and check the
manifest before copying selected files into a working checkout. Credentials must
come from the existing local environment or be supplied separately.

## Completed results and limitations

- [Handelsbanken company analysis](HANDELSBANKEN_ANALYSIS.md): source-audited company
  report, guided source collection, jobs and external links. Its original autonomous
  diagnostic was explicitly stopped early and remains marked partial.
- [Low/high comparison](HANDELSBANKEN_REASONING_COMPARISON.md): all experiment parts
  finished. High retained 36/38 technology identity controls versus 31/38 for low;
  this is selected retention, not overall accuracy. Scope and taxonomy errors remain.
- [Research checkpoint](../../../codex-sd-examples/RESEARCH_CHECKPOINT.md): earlier experiments, current
  architecture, paused PDF/OCR work and known next steps.

The preceding runtime validation passed 144 regular tests and four browser tests;
the reasoning benchmark harness and audit passed their lint/type checks. Preparing
this snapshot changes documentation only and does not rerun paid experiments.

The archive covers the state immediately before this receipt was created. Its
external manifest can also record the resulting Git checkpoint commit, without
changing the archived evidence or its checksum.
