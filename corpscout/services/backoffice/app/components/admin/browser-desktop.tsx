import { useEffect, useRef, useState } from "react";
import type RFB from "@novnc/novnc";

export function BrowserDesktop({ url }: { url: string | null }) {
  const container = useRef<HTMLDivElement>(null);
  const [connection, setConnection] = useState("Disconnected");
  useEffect(() => {
    if (!url || !container.current) return;
    let client: RFB | undefined;
    let disposed = false;
    setConnection("Connecting…");
    import("@novnc/novnc").then(({ default: RFBClient }) => {
      if (disposed || !container.current) return;
      client = new RFBClient(container.current, url);
      client.scaleViewport = true;
      client.resizeSession = false;
      client.focusOnClick = true;
      client.addEventListener("connect", () => setConnection("Connected · keyboard and mouse enabled"));
      client.addEventListener("disconnect", () => setConnection("Disconnected · reconnect to continue"));
      client.addEventListener("securityfailure", () => setConnection("Connection rejected · reconnect to get a new ticket"));
    }).catch(() => setConnection("Unable to load the browser client."));
    return () => { disposed = true; client?.disconnect(); };
  }, [url]);
  return <div className="flex flex-col gap-2">
    <p className="text-sm text-muted-foreground" role="status">{url ? connection : "Open the browser to connect."}</p>
    {url && <div ref={container} className="aspect-[36/25] w-full overflow-hidden rounded-md bg-muted" />}
  </div>;
}
