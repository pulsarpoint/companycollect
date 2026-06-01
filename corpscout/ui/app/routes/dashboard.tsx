import { useEffect, useState } from "react";
import { BarChart3, Building2, CheckSquare, Globe } from "lucide-react";
import { api } from "~/lib/api";
import type { StatsResponse } from "~/types/api";
import { StatsCard } from "~/components/app/StatsCard";
import { Skeleton } from "~/components/ui/skeleton";
import { Alert, AlertDescription } from "~/components/ui/alert";

export default function DashboardPage() {
  const [stats, setStats] = useState<StatsResponse>();
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string>();

  useEffect(() => {
    api.getStats()
      .then(setStats)
      .catch(() => setError("Failed to load dashboard data."))
      .finally(() => setLoading(false));
  }, []);

  if (loading) {
    return (
      <div className="space-y-4">
        <div className="grid grid-cols-2 gap-4 sm:grid-cols-4">
          {Array.from({ length: 6 }).map((_, i) => (
            <Skeleton key={i} className="h-24 w-full" />
          ))}
        </div>
      </div>
    );
  }

  if (error || !stats) {
    return (
      <Alert variant="destructive">
        <AlertDescription>{error ?? "Unknown error"}</AlertDescription>
      </Alert>
    );
  }

  return (
    <div className="space-y-6">
      <h1 className="text-xl font-semibold">Dashboard</h1>

      <div className="grid grid-cols-2 gap-4 sm:grid-cols-4">
        <StatsCard title="Total Companies" value={stats.total_companies} icon={Building2} />
        <StatsCard title="Total Domains" value={stats.total_domains} icon={Globe} />
        <StatsCard title="Active Domains" value={stats.active_domains} icon={BarChart3} variant="success" />
        <StatsCard title="Pending Review" value={stats.pending_review} icon={CheckSquare} href="/review" />
      </div>

      <div className="grid grid-cols-2 gap-4 sm:grid-cols-4">
        <StatsCard title="Enabled Sources" value={stats.enabled_sources} />
        <StatsCard title="Pending Raw Inputs" value={stats.pending_raw_inputs} />
      </div>
    </div>
  );
}
