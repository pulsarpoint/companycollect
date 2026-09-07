import { timingSafeEqual } from "node:crypto";
import type { Route } from "./+types/admin-api-technology-submissions";
import { submitTechnologyProposals } from "~/lib/technology-proposals.server";
import { TechnologyValidationError } from "~/lib/technology-proposals";

export async function action({ request }: Route.ActionArgs) {
  if (request.method !== "POST")
    return Response.json({ error: "Use POST." }, { status: 405 });
  const token = process.env.TECHNOLOGY_SUBMISSION_TOKEN;
  if (!token || token.length < 24)
    return Response.json(
      { error: "Technology submission is not configured." },
      { status: 503 },
    );
  const supplied = Buffer.from(request.headers.get("authorization") ?? "");
  const expected = Buffer.from(`Bearer ${token}`);
  if (
    supplied.length !== expected.length ||
    !timingSafeEqual(supplied, expected)
  ) {
    return Response.json({ error: "Unauthorized." }, { status: 401 });
  }
  if (!request.headers.get("content-type")?.startsWith("application/json")) {
    return Response.json({ error: "Use application/json." }, { status: 415 });
  }
  const reader = request.body?.getReader();
  if (!reader)
    return Response.json(
      { error: "Submission body is required." },
      { status: 400 },
    );
  const chunks: Uint8Array[] = [];
  let size = 0;
  while (true) {
    const chunk = await reader.read();
    if (chunk.done) break;
    size += chunk.value.length;
    if (size > 8_000_000) {
      await reader.cancel();
      return Response.json(
        { error: "Submission exceeds 8 MB." },
        { status: 413 },
      );
    }
    chunks.push(chunk.value);
  }
  try {
    const result = await submitTechnologyProposals(
      JSON.parse(Buffer.concat(chunks).toString("utf8")),
    );
    return Response.json(result);
  } catch (error) {
    if (
      error instanceof SyntaxError ||
      error instanceof TechnologyValidationError
    ) {
      return Response.json(
        {
          error: error instanceof SyntaxError ? "Invalid JSON." : error.message,
        },
        { status: 400 },
      );
    }
    console.error("Technology proposal submission failed", {
      errorType: error instanceof Error ? error.name : "unknown",
    });
    return Response.json(
      { error: "Could not store technology proposals." },
      { status: 500 },
    );
  }
}
