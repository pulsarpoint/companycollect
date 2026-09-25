import { importLegacyLlmProfiles } from "../app/lib/llm-settings.server";
import { llmControl } from "../app/lib/llm-control.server";

try {
  console.log(`Imported ${await importLegacyLlmProfiles()} encrypted LLM profiles into PostgreSQL.`);
} catch {
  console.error("LLM catalog import failed. The SQLite source is unchanged; check migrations, permissions, and the shared encryption key.");
  process.exitCode = 1;
} finally {
  await llmControl().end();
}
