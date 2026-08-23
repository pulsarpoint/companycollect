import { getLatestSwedenPeopleDraftTwoJob } from "~/lib/sweden-people-draft-two.server";

export async function loader() {
  return Response.json(
    { job: await getLatestSwedenPeopleDraftTwoJob() },
    { headers: { "Cache-Control": "no-store" } },
  );
}
