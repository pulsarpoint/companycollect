import { proxyActivities } from "@temporalio/workflow";
import type * as activities from "./sweden-people-draft-activities";
import type { SwedenPeopleDraftWorkflowInput } from "./sweden-people-draft-types";

const draftActivities = proxyActivities<typeof activities>({
  startToCloseTimeout: "8 hours",
  heartbeatTimeout: "5 minutes",
  retry: {
    maximumAttempts: 3,
    initialInterval: "30 seconds",
    backoffCoefficient: 2,
    maximumInterval: "5 minutes",
  },
});

export async function swedenPeopleDraftOneWorkflow(
  input: SwedenPeopleDraftWorkflowInput,
): Promise<void> {
  await draftActivities.rebuildSwedenPeopleDraftOne(input);
}

export async function swedenPeopleDraftTwoWorkflow(
  input: SwedenPeopleDraftWorkflowInput,
): Promise<void> {
  await draftActivities.rebuildSwedenPeopleDraftTwo(input);
}
