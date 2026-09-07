import { afterEach, expect, it, vi } from "vitest";
const submit = vi.hoisted(() => vi.fn());
vi.mock("~/lib/technology-proposals.server", () => ({
  submitTechnologyProposals: submit,
}));
const { action } = await import("~/routes/admin-api-technology-submissions");
const original = process.env.TECHNOLOGY_SUBMISSION_TOKEN;
afterEach(() => {
  if (original === undefined) delete process.env.TECHNOLOGY_SUBMISSION_TOKEN;
  else process.env.TECHNOLOGY_SUBMISSION_TOKEN = original;
  vi.clearAllMocks();
});
function call(body: string, authorization?: string) {
  return action({
    request: new Request("http://localhost/admin/api/technology-submissions", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...(authorization ? { authorization } : {}),
      },
      body,
    }),
  } as Parameters<typeof action>[0]);
}
it("fails closed until a submission token is configured", async () => {
  delete process.env.TECHNOLOGY_SUBMISSION_TOKEN;
  expect((await call("{}")).status).toBe(503);
  expect(submit).not.toHaveBeenCalled();
});
it("requires the configured token before parsing or storing submissions", async () => {
  process.env.TECHNOLOGY_SUBMISSION_TOKEN =
    "test-submission-token-with-enough-length";
  expect((await call("invalid JSON", "Bearer wrong-token")).status).toBe(401);
  expect(submit).not.toHaveBeenCalled();
});
it("accepts authorized JSON and returns backend validation failures safely", async () => {
  process.env.TECHNOLOGY_SUBMISSION_TOKEN =
    "test-submission-token-with-enough-length";
  const auth = `Bearer ${process.env.TECHNOLOGY_SUBMISSION_TOKEN}`;
  expect((await call("invalid JSON", auth)).status).toBe(400);
  submit.mockResolvedValue({ proposed: 1, matched: 0 });
  expect(await (await call('{"schema_version":"1.0"}', auth)).json()).toEqual({
    proposed: 1,
    matched: 0,
  });
});
