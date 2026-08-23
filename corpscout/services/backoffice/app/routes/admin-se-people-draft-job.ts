import {
  getLatestSwedenPeopleDraftInitializationJob,
} from "~/lib/sweden-people-draft.server";

export async function loader() {
  return Response.json(
    { job: await getLatestSwedenPeopleDraftInitializationJob() },
    { headers: { "Cache-Control": "no-store" } },
  );
}
