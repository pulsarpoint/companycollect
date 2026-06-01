import { NavLink, Outlet } from "react-router";
import { useEffect, useState } from "react";
import { api } from "~/lib/api";
import { cn } from "~/lib/utils";

const TABS = [
  { label: "New Companies", to: "/review/companies" },
  { label: "Domain Candidates", to: "/review/domains" },
  { label: "Financial Suggestions", to: "/review/financials" },
] as const;

export default function ReviewLayout() {
  const [pendingDomains, setPendingDomains] = useState<number | null>(null);
  const [pendingRaw, setPendingRaw] = useState<number | null>(null);

  useEffect(() => {
    api.getStats()
      .then((s) => {
        setPendingDomains(s.pending_review);
        setPendingRaw(s.pending_raw_inputs);
      })
      .catch(() => {});
  }, []);

  return (
    <div>
      <h1 className="mb-4 text-xl font-semibold">Review Queue</h1>
      <nav className="mb-6 flex gap-1 border-b">
        {TABS.map((tab) => (
          <NavLink
            key={tab.to}
            to={tab.to}
            className={({ isActive }) =>
              cn(
                "relative flex items-center gap-2 px-4 py-2 text-sm font-medium transition-colors",
                "hover:text-foreground",
                isActive
                  ? "border-b-2 border-primary text-foreground"
                  : "text-muted-foreground",
              )
            }
          >
            {tab.label}
            {tab.to === "/review/companies" && pendingRaw != null && pendingRaw > 0 && (
              <span className="rounded-full bg-primary px-2 py-0.5 text-xs font-medium text-primary-foreground">
                {pendingRaw.toLocaleString()}
              </span>
            )}
            {tab.to === "/review/domains" && pendingDomains != null && pendingDomains > 0 && (
              <span className="rounded-full bg-primary px-2 py-0.5 text-xs font-medium text-primary-foreground">
                {pendingDomains.toLocaleString()}
              </span>
            )}
          </NavLink>
        ))}
      </nav>
      <Outlet />
    </div>
  );
}
