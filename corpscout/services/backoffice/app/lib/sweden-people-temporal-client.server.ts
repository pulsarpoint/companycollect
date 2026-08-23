import { Client, Connection } from "@temporalio/client";
import { SWEDEN_PEOPLE_TASK_QUEUE } from "~/temporal/sweden-people-profile-types";

const temporalGlobal = globalThis as typeof globalThis & {
  swedenPeopleTemporalConnection?: Promise<Connection>;
};

export function swedenPeopleTemporalNamespace(): string {
  return process.env.TEMPORAL_NAMESPACE?.trim() || "corpscout";
}

export function swedenPeopleTemporalTaskQueue(): string {
  return (
    process.env.TEMPORAL_PEOPLE_TASK_QUEUE?.trim() ||
    process.env.TEMPORAL_PEOPLE_PROFILE_TASK_QUEUE?.trim() ||
    SWEDEN_PEOPLE_TASK_QUEUE
  );
}

export async function swedenPeopleTemporalClient(): Promise<Client> {
  temporalGlobal.swedenPeopleTemporalConnection ??= Connection.connect({
    address: process.env.TEMPORAL_ADDRESS?.trim() || "localhost:7233",
  });
  return new Client({
    connection: await temporalGlobal.swedenPeopleTemporalConnection,
    namespace: swedenPeopleTemporalNamespace(),
  });
}
