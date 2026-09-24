import { existsSync } from "node:fs";

export function assertWebtechAvailable() {
  if (existsSync("/tmp/corpscout-webtech-readers-paused")) {
    throw new Response("Webtech storage is being upgraded. Please retry shortly.", { status: 503 });
  }
}
