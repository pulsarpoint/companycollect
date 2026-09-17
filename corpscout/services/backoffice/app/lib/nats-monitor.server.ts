/**
 * Reads the NATS server's HTTP monitoring endpoint (unauthenticated, read-only)
 * and normalizes it for the admin page. Two requests: /varz for the server,
 * /jsz for JetStream with every stream, its config, and its consumers.
 */
import type {
  NatsConsumer,
  NatsJetStreamInfo,
  NatsMonitorResult,
  NatsServerInfo,
  NatsSnapshot,
  NatsStream,
} from "~/lib/nats-monitor";

const DEFAULT_TIMEOUT_MS = 5_000;

export interface NatsMonitorOptions {
  /** Overrides NATS_MONITOR_URL. */
  url?: string;
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
  /** Epoch milliseconds; injectable so a snapshot's checkedAt is testable. */
  now?: () => number;
}

// Only the fields the page shows. The server sends more; the captured payloads
// in tests/fixtures are the reference.
interface Varz {
  server_name: string;
  version: string;
  go: string;
  port: number;
  auth_required?: boolean;
  max_connections: number;
  max_payload: number;
  start: string;
  uptime: string;
  config_load_time?: string;
  mem: number;
  cores: number;
  cpu: number;
  connections: number;
  total_connections: number;
  subscriptions: number;
  slow_consumers: number;
  stale_connections?: number;
  in_msgs: number;
  out_msgs: number;
  in_bytes: number;
  out_bytes: number;
}

interface JszConsumer {
  name: string;
  created: string;
  config: {
    durable_name?: string;
    deliver_subject?: string;
    deliver_policy: string;
    ack_policy: string;
    ack_wait?: number;
    max_deliver?: number;
    max_ack_pending?: number;
    filter_subject?: string;
    filter_subjects?: string[];
  };
  delivered: { stream_seq: number; last_active?: string };
  ack_floor: { stream_seq: number; last_active?: string };
  num_ack_pending: number;
  num_redelivered: number;
  num_waiting: number;
  num_pending: number;
}

interface JszStream {
  name: string;
  created: string;
  config: {
    subjects?: string[];
    retention: string;
    storage: string;
    discard: string;
    compression?: string;
    num_replicas: number;
    max_consumers: number;
    max_msgs: number;
    max_bytes: number;
    max_age: number;
    max_msgs_per_subject: number;
    max_msg_size?: number;
    duplicate_window?: number;
  };
  state: {
    messages: number;
    bytes: number;
    first_seq: number;
    first_ts?: string;
    last_seq: number;
    last_ts?: string;
    num_subjects?: number;
  };
  consumer_detail?: JszConsumer[];
}

interface Jsz {
  disabled?: boolean;
  memory: number;
  storage: number;
  reserved_memory: number;
  reserved_storage: number;
  accounts: number;
  api: { total: number; errors: number };
  config: { max_memory: number; max_storage: number; store_dir: string };
  streams: number;
  consumers: number;
  messages: number;
  bytes: number;
  account_details?: { name: string; stream_detail?: JszStream[] }[];
}

/** The server spells "no limit" as -1 (or 0 for a duration). */
function limit(value: number | undefined): number | null {
  return value === undefined || value <= 0 ? null : value;
}

/** Go's zero time means "never happened". */
function timestamp(value: string | undefined): string | null {
  return value === undefined || value.startsWith("0001-01-01") ? null : value;
}

function normalizeConsumer(consumer: JszConsumer): NatsConsumer {
  const { config } = consumer;
  return {
    name: consumer.name,
    createdAt: consumer.created,
    durable: Boolean(config.durable_name),
    mode: config.deliver_subject ? "push" : "pull",
    ackPolicy: config.ack_policy,
    deliverPolicy: config.deliver_policy,
    ackWaitNanos: limit(config.ack_wait),
    maxDeliver: limit(config.max_deliver),
    maxAckPending: limit(config.max_ack_pending),
    filterSubjects:
      config.filter_subjects ?? (config.filter_subject ? [config.filter_subject] : []),
    unprocessed: consumer.num_pending,
    inFlight: consumer.num_ack_pending,
    redelivered: consumer.num_redelivered,
    waitingPulls: consumer.num_waiting,
    deliveredSequence: consumer.delivered.stream_seq,
    ackFloorSequence: consumer.ack_floor.stream_seq,
    lastDeliveredAt: timestamp(consumer.delivered.last_active),
    lastAckedAt: timestamp(consumer.ack_floor.last_active),
  };
}

function normalizeStream(account: string, stream: JszStream): NatsStream {
  const { config, state } = stream;
  return {
    account,
    name: stream.name,
    createdAt: stream.created,
    subjects: config.subjects ?? [],
    storage: config.storage,
    retention: config.retention,
    discard: config.discard,
    compression: config.compression ?? "none",
    replicas: config.num_replicas,
    maxBytes: limit(config.max_bytes),
    maxMessages: limit(config.max_msgs),
    maxMessagesPerSubject: limit(config.max_msgs_per_subject),
    maxMessageBytes: limit(config.max_msg_size),
    maxConsumers: limit(config.max_consumers),
    maxAgeNanos: limit(config.max_age),
    duplicateWindowNanos: limit(config.duplicate_window),
    messages: state.messages,
    bytes: state.bytes,
    firstSequence: state.first_seq,
    lastSequence: state.last_seq,
    firstMessageAt: timestamp(state.first_ts),
    lastMessageAt: timestamp(state.last_ts),
    subjectCount: state.num_subjects ?? 0,
    consumers: (stream.consumer_detail ?? [])
      .map(normalizeConsumer)
      .sort((a, b) => a.name.localeCompare(b.name)),
  };
}

function normalizeServer(varz: Varz): NatsServerInfo {
  return {
    name: varz.server_name,
    version: varz.version,
    goVersion: varz.go,
    startedAt: varz.start,
    uptime: varz.uptime,
    configLoadedAt: timestamp(varz.config_load_time),
    clientPort: varz.port,
    authRequired: varz.auth_required ?? false,
    cores: varz.cores,
    cpuPercent: varz.cpu,
    memoryBytes: varz.mem,
    connections: varz.connections,
    totalConnections: varz.total_connections,
    maxConnections: varz.max_connections,
    subscriptions: varz.subscriptions,
    slowConsumers: varz.slow_consumers,
    staleConnections: varz.stale_connections ?? 0,
    inMessages: varz.in_msgs,
    outMessages: varz.out_msgs,
    inBytes: varz.in_bytes,
    outBytes: varz.out_bytes,
    maxPayloadBytes: varz.max_payload,
  };
}

function normalizeJetStream(jsz: Jsz): NatsJetStreamInfo {
  return {
    storeDir: jsz.config.store_dir,
    fileUsedBytes: jsz.storage,
    fileLimitBytes: jsz.config.max_storage,
    fileReservedBytes: jsz.reserved_storage,
    memoryUsedBytes: jsz.memory,
    memoryLimitBytes: jsz.config.max_memory,
    memoryReservedBytes: jsz.reserved_memory,
    accounts: jsz.accounts,
    streams: jsz.streams,
    consumers: jsz.consumers,
    messages: jsz.messages,
    bytes: jsz.bytes,
    apiRequests: jsz.api.total,
    apiErrors: jsz.api.errors,
  };
}

function normalizeStreams(jsz: Jsz): NatsStream[] {
  return (jsz.account_details ?? [])
    .flatMap((account) =>
      (account.stream_detail ?? []).map((stream) => normalizeStream(account.name, stream)),
    )
    .sort((a, b) => a.name.localeCompare(b.name));
}

async function getJson<T>(
  url: string,
  doFetch: typeof fetch,
  timeoutMs: number,
): Promise<T> {
  const response = await doFetch(url, { signal: AbortSignal.timeout(timeoutMs) });
  if (!response.ok) throw new Error(`answered HTTP ${response.status}`);
  return (await response.json()) as T;
}

/**
 * One snapshot of the server. Never throws for a missing setting or a server
 * that does not answer: both come back as `error`, because a monitoring page
 * that fails when the thing it monitors is down would be useless.
 */
export async function loadNatsMonitor(
  options: NatsMonitorOptions = {},
): Promise<NatsMonitorResult> {
  const monitorUrl = (options.url ?? process.env.NATS_MONITOR_URL ?? "")
    .trim()
    .replace(/\/+$/, "");
  if (monitorUrl === "") {
    return {
      snapshot: null,
      error: {
        kind: "not_configured",
        message:
          "Set NATS_MONITOR_URL on the backoffice to the server's HTTP monitoring endpoint, for example http://192.168.88.129:8222.",
      },
    };
  }

  const doFetch = options.fetchImpl ?? fetch;
  const timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  try {
    const [varz, jsz] = await Promise.all([
      getJson<Varz>(`${monitorUrl}/varz`, doFetch, timeoutMs),
      getJson<Jsz>(`${monitorUrl}/jsz?consumers=true&config=true`, doFetch, timeoutMs),
    ]);
    const snapshot: NatsSnapshot = {
      checkedAt: Math.floor((options.now ?? Date.now)() / 1000),
      monitorUrl,
      server: normalizeServer(varz),
      jetstream: jsz.disabled ? null : normalizeJetStream(jsz),
      streams: jsz.disabled ? [] : normalizeStreams(jsz),
    };
    return { snapshot, error: null };
  } catch (error) {
    return {
      snapshot: null,
      error: {
        kind: "unreachable",
        message: `NATS monitoring at ${monitorUrl} did not answer: ${
          error instanceof Error ? error.message : String(error)
        }`,
      },
    };
  }
}
