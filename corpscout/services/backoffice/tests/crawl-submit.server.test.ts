import { spawn, type ChildProcess } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { connect, type NatsConnection } from "@nats-io/transport-node";
import { jetstreamManager, StorageType } from "@nats-io/jetstream";
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import { publishTestCrawl } from "~/lib/crawl-submit.server";

describe.skipIf(!process.env.NATS_SERVER)("crawl publisher with real JetStream", () => {
  let broker: ChildProcess;
  let client: NatsConnection;
  let directory: string;
  let url: string;
  let stream: string;
  let subject: string;
  let ordinal = 0;
  const payload = {request_id: "backoffice-test", url: "https://novelic.com/", crawl: "full", config: {max_pages: 5}};

  beforeAll(async () => {
    directory = await mkdtemp(join(tmpdir(), "backoffice-jetstream-"));
    const listener = createServer();
    await new Promise<void>(resolve => listener.listen(0, "127.0.0.1", resolve));
    const address = listener.address();
    if (!address || typeof address === "string") throw new Error("Missing test port");
    const port = address.port;
    await new Promise<void>((resolve, reject) => listener.close(error => error ? reject(error) : resolve()));
    broker = spawn(process.env.NATS_SERVER!, ["-js", "-a", "127.0.0.1", "-p", String(port), "-sd", directory, "--user", "test-user", "--pass", "private-password"], {stdio: "ignore"});
    url = `nats://test-user:private-password@127.0.0.1:${port}`;
    for (let attempt = 0; ; attempt++) {
      try { client = await connect({servers: `127.0.0.1:${port}`, user: "test-user", pass: "private-password", reconnect: false, timeout: 200}); break; }
      catch (error) {
        if (attempt >= 50) throw error;
        await new Promise(resolve => setTimeout(resolve, 50));
      }
    }
  });
  afterAll(async () => {
    if (client) await client.close();
    if (broker && broker.exitCode === null) {
      await new Promise<void>(resolve => {broker.once("exit", () => resolve()); broker.kill();});
    }
    if (directory) await rm(directory, {recursive: true, force: true});
  });
  beforeEach(async () => {
    ordinal++;
    stream = `CRAWL_TEST_${ordinal}`;
    subject = `crawl.test.${ordinal}`;
    vi.stubEnv("CRAWLER_TEST_SUBMIT_ENABLED", "true");
    vi.stubEnv("CRAWLER_API_URL", "http://crawler.test");
    vi.stubEnv("CRAWLER_API_TOKEN", "private-api-token");
    vi.stubEnv("NATS_URL", url);
    vi.stubEnv("NATS_CREDENTIALS", "");
    vi.stubEnv("CRAWLER_NATS_STREAM", stream);
    vi.stubEnv("CRAWLER_NATS_SUBJECT", subject);
    vi.stubGlobal("fetch", vi.fn(async () => Response.json(payload)));
    await (await jetstreamManager(client)).streams.add({name: stream, subjects: [subject], storage: StorageType.File});
  });
  afterEach(() => {vi.unstubAllGlobals(); vi.unstubAllEnvs();});

  it("publishes the validated payload durably and deduplicates an identical retry", async () => {
    const first = await publishTestCrawl(JSON.stringify({...payload, url: "novelic.com"}));
    const duplicate = await publishTestCrawl(JSON.stringify(payload));
    expect(first).toMatchObject({stream, subject, request_id: payload.request_id, sequence: 1, duplicate: false});
    expect(duplicate).toMatchObject({sequence: 1, duplicate: true});
    const manager = await jetstreamManager(client);
    const stored = await manager.streams.getMessage(stream, {seq: first.sequence});
    if (!stored) throw new Error("JetStream did not retain the acknowledged request");
    expect(stored.json()).toEqual(payload);
    expect(stored.header.get("Nats-Msg-Id")).toContain(`${payload.request_id}:`);
    expect((await manager.streams.info(stream)).state.messages).toBe(1);
    const [endpoint, options] = vi.mocked(fetch).mock.calls[0];
    expect(String(endpoint)).toBe("http://crawler.test/v1/crawls/validate");
    expect(new Headers(options?.headers).get("Authorization")).toBe("Bearer private-api-token");
    expect(JSON.stringify(first)).not.toContain("private");
  });

  it("rejects disabled, malformed and schema-invalid requests without publishing", async () => {
    vi.stubEnv("CRAWLER_TEST_SUBMIT_ENABLED", "false");
    await expect(publishTestCrawl(JSON.stringify(payload))).rejects.toThrow("disabled");
    vi.stubEnv("CRAWLER_TEST_SUBMIT_ENABLED", "true");
    await expect(publishTestCrawl("{")).rejects.toThrow("valid JSON");
    await expect(publishTestCrawl("{}")).rejects.toThrow("request_id");
    vi.stubGlobal("fetch", vi.fn(async () => Response.json({detail: [{loc: ["body", "config", "max_pages"], msg: "Must be positive", input: "private-input"}]}, {status: 422})));
    await expect(publishTestCrawl(JSON.stringify(payload))).rejects.toThrow("body.config.max_pages: Must be positive");
    expect((await (await jetstreamManager(client)).streams.info(stream)).state.messages).toBe(0);
  });

  it("does not accept a receipt from another stream or expose connection credentials", async () => {
    vi.stubEnv("CRAWLER_NATS_STREAM", "WRONG_STREAM");
    await expect(publishTestCrawl(JSON.stringify(payload))).rejects.toThrow("No JetStream receipt received");
    expect((await (await jetstreamManager(client)).streams.info(stream)).state.messages).toBe(0);
    vi.stubEnv("NATS_URL", url.replace("private-password", "wrong-private-password"));
    await expect(publishTestCrawl(JSON.stringify(payload))).rejects.toThrow("No JetStream receipt received");
  });
});
