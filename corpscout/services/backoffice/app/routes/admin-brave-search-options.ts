import { data } from "react-router";
import { BraveSearchError, loadBraveSearchOptions } from "~/lib/brave-searches.server";

export async function loader({request}: {request: Request}) {
  try {
    return {...await loadBraveSearchOptions(new URL(request.url).searchParams.get("task") ?? ""), error: null};
  } catch (error) {
    return data({searches: [], frozen: null, error: error instanceof BraveSearchError ? error.message : "Brave searches are unavailable. Please retry."},
      {status: error instanceof BraveSearchError ? 400 : 503});
  }
}
