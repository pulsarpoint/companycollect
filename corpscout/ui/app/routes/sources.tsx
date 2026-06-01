import { useEffect, useState } from "react";
import { api } from "~/lib/api";
import type { DataSource } from "~/types/api";
import { SourcesTable } from "~/components/app/SourcesTable";
import { Skeleton } from "~/components/ui/skeleton";
import { Alert, AlertDescription } from "~/components/ui/alert";

export default function SourcesPage() {
  const [sources, setSources] = useState<DataSource[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string>();

  useEffect(() => {
    api.getSources()
      .then(setSources)
      .catch(() => setError("Failed to load sources."))
      .finally(() => setLoading(false));
  }, []);

  if (loading) return <div className="space-y-2">{Array.from({ length: 5 }).map((_, i) => <Skeleton key={i} className="h-16 w-full" />)}</div>;
  if (error) return <Alert variant="destructive"><AlertDescription>{error}</AlertDescription></Alert>;

  return (
    <div>
      <h1 className="mb-4 text-xl font-semibold">Sources</h1>
      <SourcesTable sources={sources} />
    </div>
  );
}
