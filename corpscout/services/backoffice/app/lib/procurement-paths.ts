/** URL slug for a source: `sweden_uhm_procurement` → `sweden-uhm`. Kept out of
 * the database because it is a routing concern, and derived rather than stored
 * so the two cannot drift.
 *
 * Lives outside procurements.server.ts because route components call it to
 * build links. A `.server` module reaching the client bundle is a build error
 * in React Router, and this function is pure string work with no server
 * dependency. */
export function sourceSlugToPath(sourceSlug: string): string {
  return sourceSlug.replace(/_procurement$/, "").replace(/_/g, "-");
}
