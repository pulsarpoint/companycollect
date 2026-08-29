import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  loadTechnologyIconRef: vi.fn(),
  fetchObject: vi.fn(),
}));

vi.mock("~/lib/technology-catalog.server", () => ({
  loadTechnologyIconRef: mocks.loadTechnologyIconRef,
}));
vi.mock("~/lib/object-store.server", () => ({
  fetchObject: mocks.fetchObject,
}));

const { loader } = await import("~/routes/tech-icon");

const SVG = `<svg xmlns="http://www.w3.org/2000/svg"></svg>`;

function run(slug: string, headers?: HeadersInit): Promise<Response> {
  return loader({
    params: { slug },
    request: new Request(`http://localhost/icons/tech/${slug}`, { headers }),
    context: {},
  } as never) as Promise<Response>;
}

async function statusOf(run: Promise<Response>): Promise<number> {
  try {
    await run;
  } catch (thrown) {
    expect(thrown).toBeInstanceOf(Response);
    return (thrown as Response).status;
  }
  throw new Error("loader did not throw");
}

beforeEach(() => {
  mocks.loadTechnologyIconRef.mockReset();
  mocks.fetchObject.mockReset();
});

describe("tech icon resource route", () => {
  it("404s for a slug the catalog does not know (or has no icon for)", async () => {
    mocks.loadTechnologyIconRef.mockResolvedValue(null);
    expect(await statusOf(run("no-such-slug"))).toBe(404);
    expect(mocks.fetchObject).not.toHaveBeenCalled();
  });

  it("streams the icon with stored content type, long cache, and an ETag", async () => {
    mocks.loadTechnologyIconRef.mockResolvedValue({
      objectKey: "icons/wordpress.svg",
      contentType: "image/svg+xml",
      updatedAt: "2026-08-28 10:00:00",
    });
    mocks.fetchObject.mockResolvedValue(new Response(SVG, { status: 200 }));

    const response = await run("wordpress");

    expect(response.status).toBe(200);
    expect(response.headers.get("Content-Type")).toBe("image/svg+xml");
    expect(response.headers.get("Cache-Control")).toBe(
      "public, max-age=86400",
    );
    expect(response.headers.get("ETag")).toBe(
      '"icons/wordpress.svg@2026-08-28 10:00:00"',
    );
    expect(mocks.fetchObject).toHaveBeenCalledWith(
      "technology-icons",
      "icons/wordpress.svg",
    );
    await expect(response.text()).resolves.toBe(SVG);
  });

  it("serves repeat requests from the in-memory cache without re-hitting S3", async () => {
    mocks.loadTechnologyIconRef.mockResolvedValue({
      objectKey: "icons/jquery.svg",
      contentType: "image/svg+xml",
      updatedAt: "2026-08-28 10:00:00",
    });
    mocks.fetchObject.mockResolvedValue(new Response(SVG, { status: 200 }));

    await run("jquery");
    const second = await run("jquery");

    expect(mocks.fetchObject).toHaveBeenCalledTimes(1);
    await expect(second.text()).resolves.toBe(SVG);
  });

  it("answers a matching If-None-Match with 304 and no body", async () => {
    mocks.loadTechnologyIconRef.mockResolvedValue({
      objectKey: "icons/react.svg",
      contentType: "image/svg+xml",
      updatedAt: "2026-08-28 10:00:00",
    });

    const response = await run("react", {
      "If-None-Match": '"icons/react.svg@2026-08-28 10:00:00"',
    });

    expect(response.status).toBe(304);
    expect(mocks.fetchObject).not.toHaveBeenCalled();
  });

  it("502s when the object store fails", async () => {
    mocks.loadTechnologyIconRef.mockResolvedValue({
      objectKey: "icons/broken.svg",
      contentType: "image/svg+xml",
      updatedAt: "2026-08-28 10:00:00",
    });
    mocks.fetchObject.mockResolvedValue(new Response("", { status: 500 }));
    expect(await statusOf(run("broken"))).toBe(502);
  });
});
