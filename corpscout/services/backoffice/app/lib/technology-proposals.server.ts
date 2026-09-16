import { createHash, randomUUID } from "node:crypto";
import {
  chInsertTechnologyProposals,
  chInsertTechnologyReview,
  chQuery,
} from "~/lib/clickhouse.server";
import {
  normalizeTechnologyName,
  technologyArray,
  technologyObject,
  technologyText,
  technologyUrl,
  TechnologyValidationError,
  type ProposedTechnologyRow,
  type TechnologyReview,
} from "~/lib/technology-proposals";

export interface MatchingTechnology {
  technology: string;
  description: string;
  website: string;
  category_ids: number[];
  categories: string[];
  groups: string[];
}

export async function loadMatchingCatalog() {
  const [entries, aliases] = await Promise.all([
    chQuery<MatchingTechnology>(
      "SELECT technology, description, website, category_ids, categories, groups FROM corpscout.technology_catalog FINAL ORDER BY technology LIMIT 100001",
    ),
    chQuery<{ alias_key: string; technology: string }>(
      "SELECT alias_key, technology FROM corpscout.technology_aliases WHERE review_status = 'accepted' LIMIT 100001",
    ),
  ]);
  if (!entries.length || entries.length > 100000 || aliases.length > 100000) {
    throw new TechnologyValidationError("The complete catalog is unavailable.");
  }
  return { entries, aliases };
}

export function resolveCatalogName(
  name: string,
  catalog: Awaited<ReturnType<typeof loadMatchingCatalog>>,
) {
  const exact = catalog.entries.find((entry) => entry.technology === name);
  if (exact) return exact.technology;
  const key = normalizeTechnologyName(name);
  const normalized = catalog.entries.filter(
    (entry) => normalizeTechnologyName(entry.technology) === key,
  );
  if (normalized.length === 1) return normalized[0].technology;
  if (normalized.length > 1) return null;
  const aliases = catalog.aliases.filter((alias) => alias.alias_key === key);
  return aliases.length === 1 &&
    catalog.entries.some((entry) => entry.technology === aliases[0].technology)
    ? aliases[0].technology
    : null;
}

function optionalText(value: unknown, field: string): string {
  return value === null ? "" : technologyText(value, field, 4000);
}

function optionalBoolean(value: unknown): number | null {
  if (value === null) return null;
  if (typeof value !== "boolean")
    throw new TechnologyValidationError(
      "Technology flags must be true, false, or null.",
    );
  return Number(value);
}

function categoryIds(value: unknown): number[] {
  const values = technologyArray(value, "category_ids", 20);
  if (
    values.some(
      (item) =>
        !Number.isInteger(item) || Number(item) < 0 || Number(item) > 65535,
    )
  ) {
    throw new TechnologyValidationError(
      "Category IDs must be UInt16 integers.",
    );
  }
  return Array.from(new Set(values as number[]));
}

function hashText(value: unknown, field: string): string {
  const text = technologyText(value, field, 64);
  if (!/^[a-f0-9]{64}$/.test(text))
    throw new TechnologyValidationError(`${field} must be SHA-256.`);
  return text;
}

export async function submitTechnologyProposals(input: unknown) {
  const body = technologyObject(input);
  if (body.schema_version !== "1.0")
    throw new TechnologyValidationError("Expected submission schema 1.0.");
  const runId = technologyText(body.run_id, "run_id", 100);
  const siteUrl = technologyUrl(body.site_url, "site_url");
  const model = technologyText(body.model, "model", 200);
  const records = technologyArray(body.records, "records", 500);
  const catalog = await loadMatchingCatalog();
  const rows: ProposedTechnologyRow[] = [];
  const seen = new Set<string>();
  let matched = 0;
  for (const raw of records) {
    const record = technologyObject(raw),
      data = technologyObject(record.data);
    if (
      record.evidence_status !== "source_matched" ||
      data.catalog_error != null ||
      (data.interpretation_review != null &&
        technologyObject(data.interpretation_review).supported === false)
    ) {
      throw new TechnologyValidationError(
        "Only accepted technology observations can be submitted.",
      );
    }
    const requiredReviews =
      data.required_reviews == null
        ? []
        : technologyArray(data.required_reviews, "required_reviews", 10);
    const sourceReview =
      data.interpretation_review == null
        ? {}
        : technologyObject(data.interpretation_review);
    const metadataReview =
      data.proposal_review == null
        ? {}
        : technologyObject(data.proposal_review);
    if (
      (requiredReviews.includes("source_meaning") &&
        (sourceReview.status !== "accepted" ||
          sourceReview.supported !== true)) ||
      (sourceReview.status != null && sourceReview.status !== "accepted") ||
      (requiredReviews.includes("proposal_metadata") &&
        metadataReview.status !== "accepted") ||
      (metadataReview.status != null && metadataReview.status !== "accepted")
    ) {
      throw new TechnologyValidationError(
        "Required technology reviews have not passed.",
      );
    }
    const match = technologyObject(data.catalog_match);
    if (metadataReview.metadata_sha256 != null) {
      const draft = technologyObject(match.proposed_technology);
      const metadataHash = createHash("sha256")
        .update(
          JSON.stringify(
            Object.fromEntries(
              Object.entries(draft).sort(([a], [b]) =>
                a < b ? -1 : a > b ? 1 : 0,
              ),
            ),
          ),
        )
        .digest("hex");
      if (metadataReview.metadata_sha256 !== metadataHash)
        throw new TechnologyValidationError(
          "Proposal metadata changed after review.",
        );
    }
    const observed = technologyText(data.technology, "technology", 200);
    const sources = technologyArray(record.sources, "sources", 100).map(
      (rawSource) => {
        const source = technologyObject(rawSource);
        const evidence = technologyArray(source.evidence, "evidence", 8).map(
          (fragment) =>
            technologyText(technologyObject(fragment).text, "evidence", 4000),
        );
        if (!evidence.length || source.evidence_status !== "source_matched") {
          throw new TechnologyValidationError(
            "Source evidence and its verification status are required.",
          );
        }
        return {
          url: technologyUrl(source.url, "source URL"),
          html_sha256: hashText(source.html_sha256, "html_sha256"),
          fetched_at: technologyText(source.fetched_at, "fetched_at", 100),
          evidence,
          evidence_status: String(source.evidence_status),
        };
      },
    );
    if (!sources.length)
      throw new TechnologyValidationError(
        "At least one evidence source is required.",
      );
    if (match.status === "matched") {
      if (
        !catalog.entries.some(
          (entry) => entry.technology === match.canonical_technology,
        )
      ) {
        throw new TechnologyValidationError(
          "A submitted canonical identity is absent from the current catalog.",
        );
      }
      matched++;
      continue;
    }
    if (match.status !== "proposed" || match.canonical_technology !== null) {
      throw new TechnologyValidationError(
        "Invalid technology resolution state.",
      );
    }
    const proposal = technologyObject(match.proposed_technology);
    const name = technologyText(proposal.name, "proposed name", 200);
    const website =
      proposal.website === null
        ? ""
        : technologyUrl(proposal.website, "website");
    const proposalId = createHash("sha256")
      .update(JSON.stringify([normalizeTechnologyName(name), website]))
      .digest("hex");
    if (match.proposal_id !== proposalId)
      throw new TechnologyValidationError(
        "Proposal identity does not match its metadata.",
      );
    const recordId = technologyText(record.record_id, "record_id", 100);
    if (seen.has(recordId))
      throw new TechnologyValidationError(
        "Duplicate observation in submission.",
      );
    seen.add(recordId);
    const searches = technologyArray(match.searches, "searches", 120).map(
      technologyObject,
    );
    const queries = searches.map((search) =>
      technologyText(search.query, "search query", 200),
    );
    if (
      !queries.some(
        (query) =>
          normalizeTechnologyName(query) === normalizeTechnologyName(observed),
      )
    ) {
      throw new TechnologyValidationError(
        "Proposal is missing a search for its observed name.",
      );
    }
    const ids = categoryIds(proposal.category_ids);
    const categorySuggestion = optionalText(
      proposal.category_suggestion,
      "category suggestion",
    );
    if (!ids.length && !categorySuggestion) {
      throw new TechnologyValidationError(
        "A proposal requires at least one category ID or a category suggestion.",
      );
    }
    const knownIds = new Set(
      catalog.entries.flatMap((entry) => entry.category_ids),
    );
    if (ids.some((id) => !knownIds.has(id)))
      throw new TechnologyValidationError("Unknown proposed category ID.");
    const signal = technologyText(data.signal, "signal", 100);
    const scope = technologyText(data.scope, "scope", 100);
    if (
      ![
        "stated_use",
        "advertised_expertise",
        "develops",
        "offers",
        "required_experience",
        "preferred_experience",
        "planned_adoption",
        "past_use",
        "being_replaced",
        "explicitly_not_used",
        "mentioned",
      ].includes(signal) ||
      !["company", "team", "role", "client", "unknown"].includes(scope)
    ) {
      throw new TechnologyValidationError(
        "Invalid technology signal or scope.",
      );
    }
    rows.push({
      proposal_id: proposalId,
      run_id: runId,
      record_id: recordId,
      observed_name: observed,
      proposed_name: name,
      proposed_key: normalizeTechnologyName(name),
      description: technologyText(proposal.description, "description"),
      website,
      category_ids: ids,
      category_suggestion: categorySuggestion,
      saas: optionalBoolean(proposal.saas),
      oss: optionalBoolean(proposal.oss),
      pricing: technologyArray(proposal.pricing, "pricing", 10).map((value) =>
        technologyText(value, "pricing", 100),
      ),
      company: optionalText(data.company, "company"),
      job_employer: optionalText(data.job_employer, "job employer"),
      job_title: optionalText(data.job_title, "job title"),
      job_url:
        data.job_url === null ? "" : technologyUrl(data.job_url, "job URL"),
      signal,
      scope,
      context: technologyText(data.context, "context", 4000),
      as_of: optionalText(data.as_of, "as of"),
      alternative_group: optionalText(
        data.alternative_group,
        "alternative group",
      ),
      site_url: siteUrl,
      sources,
      catalog_version: hashText(match.catalog_version, "catalog version"),
      search_queries: queries,
      search_candidates: Array.from(
        new Set(
          searches.flatMap((search) =>
            technologyArray(search.candidates, "search candidates", 10).map(
              (entry) =>
                technologyText(
                  technologyObject(entry).technology,
                  "candidate",
                  200,
                ),
            ),
          ),
        ),
      ),
      proposal_reason: technologyText(match.reason, "proposal reason", 4000),
      existing_technology:
        resolveCatalogName(observed, catalog) ??
        resolveCatalogName(name, catalog) ??
        "",
      model,
      received_at: new Date().toISOString().replace("T", " ").replace("Z", ""),
    });
  }
  // Validate the entire batch before writing any proposal; the retry key stays stable.
  await chInsertTechnologyProposals(rows);
  return {
    proposed: rows.length,
    matched,
    proposals: rows.map((row) => ({
      record_id: row.record_id,
      proposal_id: row.proposal_id,
      current_canonical_technology: row.existing_technology || null,
    })),
  };
}

export async function loadTechnologyProposals(page: number, status: string) {
  const filter =
    status === "all"
      ? "1"
      : "proposal_id NOT IN (SELECT proposal_id FROM corpscout.technology_proposal_latest_reviews)";
  return chQuery<ProposedTechnologyRow>(
    `SELECT * FROM corpscout.new_tech FINAL WHERE ${filter}
    ORDER BY received_at DESC, proposal_id LIMIT 1 BY proposal_id LIMIT 25 OFFSET {offset:UInt32}`,
    { offset: (page - 1) * 25 },
  );
}

export async function loadTechnologyProposal(proposalId: string) {
  const [observations, reviews, catalog] = await Promise.all([
    chQuery<ProposedTechnologyRow>(
      "SELECT * FROM corpscout.new_tech FINAL WHERE proposal_id = {id:String} ORDER BY received_at DESC LIMIT 100",
      { id: proposalId },
    ),
    chQuery<TechnologyReview>(
      "SELECT * FROM corpscout.technology_proposal_reviews WHERE proposal_id = {id:String} ORDER BY reviewed_at DESC, review_id DESC LIMIT 100",
      { id: proposalId },
    ),
    loadMatchingCatalog(),
  ]);
  return { observations, reviews, catalog };
}

export async function reviewTechnologyProposal(
  proposalId: string,
  form: FormData,
) {
  const { observations, reviews, catalog } =
    await loadTechnologyProposal(proposalId);
  if (!observations.length)
    throw new TechnologyValidationError("Proposal not found.");
  if (
    String(form.get("expected_review_id") ?? "") !==
    (reviews[0]?.review_id ?? "")
  ) {
    throw new TechnologyValidationError(
      "This proposal has a newer review. Reload before deciding.",
    );
  }
  const decision = String(form.get("decision"));
  if (!["approve_new", "map_existing", "reject"].includes(decision))
    throw new TechnologyValidationError("Unknown decision.");
  const review: TechnologyReview = {
    review_id: randomUUID(),
    proposal_id: proposalId,
    decision: decision as TechnologyReview["decision"],
    technology: "",
    description: "",
    website: "",
    category_ids: [],
    categories: [],
    groups: [],
    saas: 0,
    oss: 0,
    pricing: [],
    alias: "",
    alias_key: "",
    reviewed_by: technologyText(form.get("reviewed_by"), "Reviewer", 200),
    review_note: technologyText(form.get("review_note"), "Review note", 4000),
    source_references: Array.from(
      new Set(
        observations.flatMap((row) => row.sources.map((source) => source.url)),
      ),
    ),
    reviewed_at: new Date().toISOString().replace("T", " ").replace("Z", ""),
  };
  if (decision !== "reject") {
    review.technology = technologyText(
      form.get("technology"),
      "Canonical technology",
      200,
    );
    const existing = catalog.entries.find(
      (entry) => entry.technology === review.technology,
    );
    if (decision === "map_existing" && !existing)
      throw new TechnologyValidationError(
        "Select an exact existing catalog name.",
      );
    if (
      decision === "map_existing" &&
      reviews[0]?.decision === "approve_new" &&
      reviews[0].technology === review.technology
    ) {
      throw new TechnologyValidationError(
        "Keep this proposal approved as a new entry; it owns that catalog entry.",
      );
    }
    if (decision === "approve_new") {
      const collisions = catalog.entries.filter(
        (entry) =>
          normalizeTechnologyName(entry.technology) ===
          normalizeTechnologyName(review.technology),
      );
      // Re-reviewing this proposal's own published entry is allowed; another
      // proposal/catalog entry must be mapped explicitly instead of overwritten.
      if (
        collisions.length &&
        !(
          reviews[0]?.decision === "approve_new" &&
          reviews[0].technology === review.technology &&
          collisions.length === 1
        )
      ) {
        throw new TechnologyValidationError(
          "This technology already exists; map to it instead.",
        );
      }
      if (
        catalog.aliases.some(
          (alias) =>
            alias.alias_key === normalizeTechnologyName(review.technology),
        )
      ) {
        throw new TechnologyValidationError(
          "This name is already an accepted alias; map to its catalog technology.",
        );
      }
      const approved = await chQuery<{
        proposal_id: string;
        technology: string;
      }>(
        "SELECT proposal_id, technology FROM corpscout.technology_proposal_latest_reviews WHERE decision = 'approve_new'",
      );
      if (
        approved.some(
          (row) =>
            row.proposal_id !== proposalId &&
            normalizeTechnologyName(row.technology) ===
              normalizeTechnologyName(review.technology),
        )
      ) {
        throw new TechnologyValidationError(
          "Another proposal already approved this name. Publish it, then map to the catalog entry.",
        );
      }
      review.description = technologyText(
        form.get("description"),
        "Description",
      );
      review.website = technologyUrl(form.get("website"), "Official website");
      review.source_references.push(review.website);
      const ids = String(form.get("category_ids") ?? "")
        .split(",")
        .filter((value) => value.trim())
        .map(Number);
      review.category_ids = categoryIds(ids);
      if (!ids.length)
        throw new TechnologyValidationError(
          "Choose at least one existing category ID.",
        );
      for (const id of review.category_ids) {
        const categoryNames = new Set(
          catalog.entries.flatMap((entry) =>
            entry.category_ids.flatMap((value, i) =>
              value === id ? [entry.categories[i]] : [],
            ),
          ),
        );
        if (
          categoryNames.size !== 1 ||
          Array.from(categoryNames).some((name) => typeof name !== "string")
        )
          throw new TechnologyValidationError(
            `Category ${id} is missing or ambiguous.`,
          );
        review.categories.push(Array.from(categoryNames)[0]);
      }
      // Group membership belongs to each category, not to another technology's
      // aggregate group list. The publisher resolves groups from its vocabulary.
      for (const flag of ["saas", "oss"] as const) {
        const value = form.get(flag);
        if (value !== "true" && value !== "false")
          throw new TechnologyValidationError(`Choose ${flag} explicitly.`);
        review[flag] = Number(value === "true");
      }
      review.pricing = technologyArray(
        String(form.get("pricing") ?? "")
          .split(",")
          .map((value) => value.trim())
          .filter(Boolean),
        "pricing",
        10,
      ).map((value) => technologyText(value, "pricing", 100));
    }
    const alias = String(form.get("alias") ?? "").trim();
    if (alias) {
      review.alias = technologyText(alias, "Alias", 200);
      review.alias_key = normalizeTechnologyName(alias);
      if (
        catalog.entries.some(
          (entry) =>
            normalizeTechnologyName(entry.technology) === review.alias_key,
        ) ||
        review.alias_key === normalizeTechnologyName(review.technology)
      ) {
        throw new TechnologyValidationError(
          "Case variants of canonical names do not need aliases.",
        );
      }
      const aliasConflict = catalog.aliases.find(
        (entry) =>
          entry.alias_key === review.alias_key &&
          entry.technology !== review.technology,
      );
      if (aliasConflict)
        throw new TechnologyValidationError(
          "Alias already points to another technology.",
        );
      const pendingAliases = await chQuery<{
        proposal_id: string;
        technology: string;
        alias_key: string;
      }>(
        "SELECT proposal_id, technology, alias_key FROM corpscout.technology_proposal_latest_reviews WHERE decision IN ('approve_new', 'map_existing') AND alias_key = {key:String}",
        { key: review.alias_key },
      );
      if (
        pendingAliases.some(
          (entry) =>
            entry.proposal_id !== proposalId &&
            entry.technology !== review.technology,
        )
      ) {
        throw new TechnologyValidationError(
          "Another review already approved this alias for a different technology.",
        );
      }
    }
  }
  await chInsertTechnologyReview(review);
  return review;
}
