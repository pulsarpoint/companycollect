import { describe, expect, it } from "vitest";
import {
  formatAge,
  formatBytes,
  formatCount,
  formatNanos,
  streamBacklog,
  usagePercent,
  type NatsConsumer,
} from "~/lib/nats-monitor";

describe("formatBytes", () => {
  it("uses binary units, the way the server's own limits are expressed", () => {
    expect(formatBytes(0)).toBe("0 B");
    expect(formatBytes(531)).toBe("531 B");
    expect(formatBytes(1_048_576)).toBe("1 MiB");
    expect(formatBytes(21_868_544)).toBe("20.9 MiB");
    expect(formatBytes(1_073_741_824)).toBe("1 GiB");
    expect(formatBytes(96_636_764_160)).toBe("90 GiB");
  });
});

describe("formatNanos", () => {
  it("reads server durations, which are nanoseconds", () => {
    expect(formatNanos(1_000_000_000)).toBe("1s");
    expect(formatNanos(120_000_000_000)).toBe("2m");
    expect(formatNanos(5_400_000_000_000)).toBe("1h 30m");
    expect(formatNanos(604_800_000_000_000)).toBe("7d");
    expect(formatNanos(90_000_000_000_000)).toBe("1d 1h");
    expect(formatNanos(250_000_000)).toBe("250ms");
  });
});

describe("formatAge", () => {
  // 2026-09-17T20:05:00Z
  const checkedAt = 1_789_675_500;

  it("measures against the snapshot time, so server and client render the same text", () => {
    expect(formatAge("2026-09-17T20:04:49.964098897Z", checkedAt)).toBe("10s ago");
    expect(formatAge("2026-09-17T20:02:00Z", checkedAt)).toBe("3m ago");
    expect(formatAge("2026-09-17T16:10:49Z", checkedAt)).toBe("3h ago");
    expect(formatAge("2026-09-14T20:05:00Z", checkedAt)).toBe("3d ago");
  });

  it("reads a timestamp slightly ahead of the backoffice clock as now, not as negative", () => {
    expect(formatAge("2026-09-17T20:05:02Z", checkedAt)).toBe("just now");
    expect(formatAge("2026-09-17T20:05:00Z", checkedAt)).toBe("just now");
  });
});

describe("formatCount", () => {
  it("groups thousands", () => {
    expect(formatCount(9)).toBe("9");
    expect(formatCount(1_234_567)).toBe("1,234,567");
  });
});

describe("usagePercent", () => {
  it("is null without a limit and clamped with one", () => {
    expect(usagePercent(10, null)).toBeNull();
    expect(usagePercent(10, 0)).toBeNull();
    expect(usagePercent(531, 96_636_764_160)).toBe(0);
    expect(usagePercent(50, 200)).toBe(25);
    expect(usagePercent(300, 200)).toBe(100);
  });
});

describe("streamBacklog", () => {
  const consumer = (unprocessed: number) => ({ unprocessed }) as NatsConsumer;

  it("is the furthest-behind consumer, since each reads the stream independently", () => {
    expect(streamBacklog([consumer(4), consumer(10), consumer(0)])).toBe(10);
  });

  it("is null when nothing consumes the stream", () => {
    expect(streamBacklog([])).toBeNull();
  });
});
