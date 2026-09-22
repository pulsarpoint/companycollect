import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { connect, credsAuthenticator } from "@nats-io/transport-node";
import { jetstream } from "@nats-io/jetstream";
import { crawlerFetch } from "~/lib/crawler.server";

export async function publishTestCrawl(body: string) {
  if (process.env.CRAWLER_TEST_SUBMIT_ENABLED !== "true") {
    throw new Error("Test submissions are disabled. Set CRAWLER_TEST_SUBMIT_ENABLED=true on Backoffice.");
  }
  if (Buffer.byteLength(body) > 131_072) throw new Error("The request must be 128 KiB or smaller.");
  let payload: unknown;
  try { payload = JSON.parse(body); }
  catch { throw new Error("Enter a valid JSON request."); }
  if (!payload || typeof payload !== "object" || Array.isArray(payload)
    || !("request_id" in payload) || typeof payload.request_id !== "string" || !payload.request_id) {
    throw new Error("Include a request_id in the JSON. Keep it unchanged when retrying a submission.");
  }
  const server = process.env.NATS_URL;
  if (!server) throw new Error("Configure NATS_URL on Backoffice.");
  const stream = process.env.CRAWLER_NATS_STREAM || "COMPANY_CRAWL";
  const subject = process.env.CRAWLER_NATS_SUBJECT || "company.crawl.requests";
  if (!/^[A-Za-z0-9_-]+$/.test(stream) || !/^[A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+)*$/.test(subject)) {
    throw new Error("Configure a valid CRAWLER_NATS_STREAM and CRAWLER_NATS_SUBJECT.");
  }

  // The crawler owns its request schema and defaults, including config overrides.
  const response = await crawlerFetch("/v1/crawls/validate", {
    method: "POST", headers: {"Content-Type": "application/json"}, body,
  });
  const request = await response.json() as {request_id: string; url: string};
  const encoded = JSON.stringify(request);
  const digest = createHash("sha256").update(encoded).digest("hex");
  try {
    const url = new URL(server);
    const user = decodeURIComponent(url.username);
    const pass = decodeURIComponent(url.password);
    url.username = "";
    url.password = "";
    const credentials = process.env.NATS_CREDENTIALS;
    const client = await connect({
      servers: url.toString(), name: "backoffice-crawl-tests", timeout: 5_000, reconnect: false,
      ...(credentials ? {authenticator: credsAuthenticator(await readFile(credentials))}
        : pass ? {user, pass} : user ? {token: user} : {}),
    });
    try {
      const receipt = await jetstream(client).publish(subject, encoded, {
        msgID: `${request.request_id}:${digest}`, expect: {streamName: stream}, timeout: 5_000, retries: 1,
      });
      return {request_id: request.request_id, url: request.url, stream: receipt.stream,
        sequence: receipt.seq, duplicate: receipt.duplicate, subject};
    } finally { await client.close(); }
  } catch {
    // Broker errors can contain authenticated connection URLs. Never return them.
    throw new Error("No JetStream receipt received. Check the crawl history before retrying the same request and request_id; publication may have succeeded.");
  }
}
