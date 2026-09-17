/**
 * Shapes and formatters for the NATS monitoring page. Client-safe: the route
 * component renders with these, while the fetching lives in
 * `nats-monitor.server.ts`.
 *
 * Limits the server reports as "unlimited" (-1, or 0 for durations) are null
 * here, so a caller cannot mistake one for a real number.
 */

export interface NatsServerInfo {
  name: string;
  version: string;
  goVersion: string;
  startedAt: string;
  /** The server's own rendering, e.g. "3h54m2s". */
  uptime: string;
  configLoadedAt: string | null;
  clientPort: number;
  authRequired: boolean;
  cores: number;
  cpuPercent: number;
  memoryBytes: number;
  connections: number;
  totalConnections: number;
  maxConnections: number;
  subscriptions: number;
  slowConsumers: number;
  staleConnections: number;
  inMessages: number;
  outMessages: number;
  inBytes: number;
  outBytes: number;
  maxPayloadBytes: number;
}

export interface NatsJetStreamInfo {
  storeDir: string;
  fileUsedBytes: number;
  fileLimitBytes: number;
  /** Sum of every file stream's max_bytes: space promised, not yet used. */
  fileReservedBytes: number;
  memoryUsedBytes: number;
  memoryLimitBytes: number;
  memoryReservedBytes: number;
  accounts: number;
  streams: number;
  consumers: number;
  messages: number;
  bytes: number;
  apiRequests: number;
  apiErrors: number;
}

export interface NatsConsumer {
  name: string;
  createdAt: string;
  durable: boolean;
  mode: "pull" | "push";
  ackPolicy: string;
  deliverPolicy: string;
  ackWaitNanos: number | null;
  maxDeliver: number | null;
  maxAckPending: number | null;
  filterSubjects: string[];
  /** Matching messages not yet delivered to anyone: the backlog. */
  unprocessed: number;
  /** Delivered, acknowledgement still outstanding. */
  inFlight: number;
  redelivered: number;
  /** Pull requests parked on the server: workers idle, waiting for messages. */
  waitingPulls: number;
  deliveredSequence: number;
  /** Every stream sequence up to this one is acknowledged. */
  ackFloorSequence: number;
  lastDeliveredAt: string | null;
  lastAckedAt: string | null;
}

export interface NatsStream {
  account: string;
  name: string;
  createdAt: string;
  subjects: string[];
  storage: string;
  retention: string;
  discard: string;
  compression: string;
  replicas: number;
  maxBytes: number | null;
  maxMessages: number | null;
  maxMessagesPerSubject: number | null;
  maxMessageBytes: number | null;
  maxConsumers: number | null;
  maxAgeNanos: number | null;
  duplicateWindowNanos: number | null;
  messages: number;
  bytes: number;
  firstSequence: number;
  lastSequence: number;
  firstMessageAt: string | null;
  lastMessageAt: string | null;
  /** Distinct subjects currently holding messages. */
  subjectCount: number;
  consumers: NatsConsumer[];
}

export interface NatsSnapshot {
  /** Epoch seconds, from the backoffice clock. */
  checkedAt: number;
  monitorUrl: string;
  server: NatsServerInfo;
  /** Null when the server runs without JetStream. */
  jetstream: NatsJetStreamInfo | null;
  streams: NatsStream[];
}

export interface NatsMonitorError {
  kind: "not_configured" | "unreachable";
  message: string;
}

/** A down server is this page's subject, not an exception: it is a result. */
export type NatsMonitorResult =
  | { snapshot: NatsSnapshot; error: null }
  | { snapshot: null; error: NatsMonitorError };

const BYTE_UNITS = ["B", "KiB", "MiB", "GiB", "TiB"] as const;

/** Binary units, matching how the server's limits are configured (90GiB). */
export function formatBytes(bytes: number): string {
  let value = bytes;
  let unit = 0;
  while (value >= 1024 && unit < BYTE_UNITS.length - 1) {
    value /= 1024;
    unit += 1;
  }
  const rounded = unit === 0 ? value : Math.round(value * 10) / 10;
  return `${rounded.toLocaleString("en-GB")} ${BYTE_UNITS[unit]}`;
}

const NANOS_PER_MS = 1_000_000;
const DURATION_UNITS = [
  ["d", 86_400_000],
  ["h", 3_600_000],
  ["m", 60_000],
  ["s", 1_000],
  ["ms", 1],
] as const;

/** The two most significant units of a nanosecond duration: "1h 30m". */
export function formatNanos(nanos: number): string {
  let remaining = Math.round(nanos / NANOS_PER_MS);
  const parts: string[] = [];
  for (const [label, size] of DURATION_UNITS) {
    const count = Math.floor(remaining / size);
    if (count > 0) {
      parts.push(`${count}${label}`);
      remaining -= count * size;
    }
    if (parts.length === 2) break;
  }
  return parts.length > 0 ? parts.join(" ") : "0s";
}

/**
 * How long before the snapshot a server timestamp lies. Measured against
 * `checkedAt` rather than the wall clock so the server render and the client
 * hydration agree. The backoffice and NATS clocks differ slightly; a timestamp
 * a moment "ahead" is now, not a negative age.
 */
export function formatAge(timestamp: string, checkedAt: number): string {
  const seconds = Math.floor(checkedAt - Date.parse(timestamp) / 1000);
  if (seconds < 1) return "just now";
  if (seconds < 60) return `${seconds}s ago`;
  if (seconds < 3_600) return `${Math.floor(seconds / 60)}m ago`;
  if (seconds < 86_400) return `${Math.floor(seconds / 3_600)}h ago`;
  return `${Math.floor(seconds / 86_400)}d ago`;
}

export function formatCount(value: number): string {
  return value.toLocaleString("en-GB");
}

/** Whole percent of a limit, clamped to 100; null when there is no limit. */
export function usagePercent(used: number, limit: number | null): number | null {
  if (limit === null || limit <= 0) return null;
  return Math.min(100, Math.round((used / limit) * 100));
}

/**
 * Consumers read a stream independently, so the stream's backlog is its
 * furthest-behind consumer, not a sum. Null when nothing consumes it.
 */
export function streamBacklog(consumers: NatsConsumer[]): number | null {
  if (consumers.length === 0) return null;
  return Math.max(...consumers.map((consumer) => consumer.unprocessed));
}
