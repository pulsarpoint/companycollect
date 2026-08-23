import { getLatestSwedenPeopleProfileBulkJob } from "~/lib/sweden-person-profile-bulk.server";

export function loader() {
  return Response.json(
    { job: getLatestSwedenPeopleProfileBulkJob() },
    { headers: { "Cache-Control": "no-store" } },
  );
}
