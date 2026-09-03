"""Markdown report for a scored run."""

from ex4.scoring import RunScores


def render_report(scores: RunScores) -> str:
    lines = [
        f"# Run {scores.run_id}",
        "",
        f"Scored at {scores.scored_at}. Ranking: mean coverage, then junk rate, then tokens.",
        "",
        "## Ranking",
        "",
        "| prompt | sites | coverage mean | coverage min | junk | other lang | tokens | latency ms | failures | stability |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    for s in scores.prompts:
        stability = f"{s.stability:.2f}" if s.stability is not None else "n/a"
        lines.append(
            f"| {s.prompt_name} | {s.sites} | {s.mean_coverage:.2f} | {s.min_coverage:.2f} | "
            f"{s.mean_junk_rate:.2f} | {s.mean_other_language_rate:.2f} | "
            f"{s.mean_total_tokens:,.0f} | {s.mean_latency_ms:,.0f} | {s.failures} | {stability} |"
        )
    prompts = [s.prompt_name for s in scores.prompts]
    domains = sorted({r.domain for r in scores.results})
    by_cell = {(r.domain, r.prompt_name): r for r in scores.results if r.repeat == 1}
    lines += [
        "",
        "## Coverage by site (repeat 1)",
        "",
        "| site | " + " | ".join(prompts) + " |",
        "|---|" + "---|" * len(prompts),
    ]
    for domain in domains:
        cells = [
            f"{by_cell[(domain, p)].coverage:.2f}" if (domain, p) in by_cell else "-"
            for p in prompts
        ]
        lines.append(f"| {domain} | " + " | ".join(cells) + " |")
    lines += ["", "## Missed fields", ""]
    for s in scores.prompts:
        lines.append(f"### {s.prompt_name}")
        if not s.missed_by_site:
            lines.append("- none")
        for domain, missed in sorted(s.missed_by_site.items()):
            lines.append(f"- {domain}: {', '.join(missed)}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"
