import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  list: vi.fn(),
  detail: vi.fn(),
}));

vi.mock("~/lib/se-ratsit-results.server", () => ({
  listSeRatsitRequests: mocks.list,
  loadSeRatsitRequestDetail: mocks.detail,
}));

const { loader } = await import("~/routes/admin-se-companies-ratsit");

const selection = {
  companyId: "193407093016",
  batchId: "5e53617b-9263-5529-8313-70a41661beac",
};

function get(search = "") {
  return loader({
    request: new Request(
      `http://backoffice/admin/se/companies/ratsit${search}`,
    ),
  } as unknown as Parameters<typeof loader>[0]);
}

beforeEach(() => {
  mocks.list.mockReset();
  mocks.detail.mockReset();
  mocks.list.mockResolvedValue({ rows: [], total: 0, page: 1, pageSize: 50 });
  mocks.detail.mockResolvedValue(null);
});

describe("admin Ratsit route loader", () => {
  it("loads the paged request list by default", async () => {
    const result = await get("?page=2&pageSize=100");

    expect(mocks.list).toHaveBeenCalledWith({
      page: 2,
      pageSize: 100,
      sort: undefined,
      dir: undefined,
    });
    expect(mocks.detail).not.toHaveBeenCalled();
    expect(result.mode).toBe("list");
  });

  it("loads only one exact request when company and batch are selected", async () => {
    const result = await get(
      `?companyId=${selection.companyId}&batchId=${selection.batchId}`,
    );

    expect(mocks.detail).toHaveBeenCalledWith(selection);
    expect(mocks.list).not.toHaveBeenCalled();
    expect(result.mode).toBe("detail");
  });

  it("falls back to the list instead of querying an invalid UUID", async () => {
    const result = await get(
      `?companyId=${selection.companyId}&batchId=not-a-uuid`,
    );

    expect(mocks.detail).not.toHaveBeenCalled();
    expect(mocks.list).toHaveBeenCalledOnce();
    expect(result.mode).toBe("list");
  });
});
