import "dotenv/config";
import { fileURLToPath } from "node:url";
import { NativeConnection, Worker } from "@temporalio/worker";
import * as draftActivities from "./sweden-people-draft-activities";
import * as profileActivities from "./sweden-people-profile-activities";
import { SWEDEN_PEOPLE_TASK_QUEUE } from "./sweden-people-profile-types";

function positiveIntegerEnvironmentValue(
  name: string,
  fallback: number,
): number {
  const configured = Number(process.env[name]);
  return Number.isInteger(configured) && configured > 0
    ? configured
    : fallback;
}

async function run(): Promise<void> {
  const connection = await NativeConnection.connect({
    address: process.env.TEMPORAL_ADDRESS?.trim() || "localhost:7233",
  });
  try {
    const worker = await Worker.create({
      connection,
      namespace: process.env.TEMPORAL_NAMESPACE?.trim() || "corpscout",
      taskQueue:
        process.env.TEMPORAL_PEOPLE_TASK_QUEUE?.trim() ||
        process.env.TEMPORAL_PEOPLE_PROFILE_TASK_QUEUE?.trim() ||
        SWEDEN_PEOPLE_TASK_QUEUE,
      workflowsPath: fileURLToPath(
        new URL("./sweden-people-workflows.ts", import.meta.url),
      ),
      activities: { ...draftActivities, ...profileActivities },
      maxConcurrentActivityTaskExecutions: positiveIntegerEnvironmentValue(
        "TEMPORAL_PEOPLE_WORKER_ACTIVITY_SLOTS",
        positiveIntegerEnvironmentValue(
          "TEMPORAL_PEOPLE_PROFILE_WORKER_ACTIVITY_SLOTS",
          4,
        ),
      ),
    });
    await worker.run();
  } finally {
    await connection.close();
  }
}

run().catch((error: unknown) => {
  console.error("Sweden people Temporal worker stopped", { error });
  process.exitCode = 1;
});
