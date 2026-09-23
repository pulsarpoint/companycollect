import { data } from "react-router";
import { getWorkspaceIpStatistics } from "~/lib/workspace-ip-addresses.server";

export async function loader() {
  try {
    return { statistics: await getWorkspaceIpStatistics() };
  } catch (error) {
    console.error("Unable to count the DNS IP inventory", error);
    return data({ statistics: null }, { status: 503 });
  }
}
