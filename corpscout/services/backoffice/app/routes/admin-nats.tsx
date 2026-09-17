import { useEffect } from "react";
import { useRevalidator } from "react-router";
import type { Route } from "./+types/admin-nats";
import { NatsMonitorView } from "~/components/admin/nats-monitor-view";
import { loadNatsMonitor } from "~/lib/nats-monitor.server";

// A server that does not answer is a result, not a thrown error: the page has
// to render precisely when NATS is down.
export async function loader() {
  return loadNatsMonitor();
}

export function meta() {
  return [{ title: "NATS | CompanyCollect admin" }];
}

export default function AdminNats({ loaderData }: Route.ComponentProps) {
  const { revalidate, state } = useRevalidator();
  const unavailable = loaderData.error !== null;

  // Same cadence as Processing: poll while the tab is visible, slower while the
  // server is unreachable, and catch up at once when the tab comes back.
  useEffect(() => {
    const refresh = () => {
      if (document.visibilityState === "visible" && state === "idle") void revalidate();
    };
    const timer = window.setInterval(refresh, unavailable ? 15_000 : 5_000);
    document.addEventListener("visibilitychange", refresh);
    return () => {
      window.clearInterval(timer);
      document.removeEventListener("visibilitychange", refresh);
    };
  }, [unavailable, revalidate, state]);

  return (
    <NatsMonitorView
      result={loaderData}
      refreshing={state !== "idle"}
      onRefresh={() => void revalidate()}
    />
  );
}
