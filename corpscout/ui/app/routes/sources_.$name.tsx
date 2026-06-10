import { useEffect, useState } from "react";
import { Link, NavLink, Outlet, useParams } from "react-router";
import { ChevronLeft } from "lucide-react";
import { api } from "~/lib/api";
import type { DataSource } from "~/types/api";
import { Alert, AlertDescription } from "~/components/ui/alert";
import { Skeleton } from "~/components/ui/skeleton";
import { SourceHeader } from "~/components/app/source-detail/SourceHeader";
import { sourceDetailTabs } from "~/components/app/source-detail/sourceDetailUtils";
import { cn } from "~/lib/utils";

export interface SourceDetailContext {
  source: DataSource;
}

export default function SourceDetailLayout() {
  const { name } = useParams<{ name: string }>();
  const [source, setSource] = useState<DataSource>();
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string>();

  useEffect(() => {
    if (!name) return;
    let ignore = false;
    setSource(undefined);
    setLoading(true);
    setError(undefined);
    api.getSource(name)
      .then((loadedSource) => { if (!ignore) setSource(loadedSource); })
      .catch(() => { if (!ignore) setError("Source not found."); })
      .finally(() => { if (!ignore) setLoading(false); });
    return () => { ignore = true; };
  }, [name]);

  if (loading) return <Skeleton className="h-64 w-full" />;
  if (error || !source) {
    return (
      <Alert variant="destructive">
        <AlertDescription>{error ?? "Source not found."}</AlertDescription>
      </Alert>
    );
  }

  const tabs = sourceDetailTabs(source);

  const context: SourceDetailContext = { source };

  return (
    <div className="flex min-h-0 flex-1 flex-col gap-6">
      <Link
        to="/sources"
        className="inline-flex items-center gap-1 text-sm text-muted-foreground hover:underline"
      >
        <ChevronLeft className="size-4" />
        Sources
      </Link>

      <SourceHeader source={source} />

      <nav className="flex gap-1 border-b">
        {tabs.map((tab) => (
          <NavLink
            key={tab.to}
            to={tab.to}
            className={({ isActive }) =>
              cn(
                "relative px-4 py-2 text-sm font-medium transition-colors hover:text-foreground",
                isActive
                  ? "border-b-2 border-primary text-foreground"
                  : "text-muted-foreground",
              )
            }
          >
            {tab.label}
          </NavLink>
        ))}
      </nav>

      <Outlet context={context} />
    </div>
  );
}
