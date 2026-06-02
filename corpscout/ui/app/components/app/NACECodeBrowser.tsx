import { useEffect, useMemo, useState } from "react";
import { ChevronRight, Loader2 } from "lucide-react";
import { Alert, AlertDescription } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Skeleton } from "~/components/ui/skeleton";
import { api, errorMessage } from "~/lib/api";
import type { NACECode, NACERevision } from "~/types/api";

interface NACECodeBrowserProps {
  defaultRevision?: string;
  onSelect?: (code: NACECode) => void;
}

function levelLabel(value: NACECode["level_name"]) {
  switch (value) {
    case "section":
      return "Section";
    case "division":
      return "Division";
    case "group":
      return "Group";
    case "class":
      return "Class";
    default:
      return value;
  }
}

function chooseInitialRevision(
  revisions: NACERevision[],
  preferredRevision: string,
) {
  return (
    revisions.find((revision) => revision.revision === preferredRevision)
      ?.revision ??
    revisions.at(-1)?.revision ??
    preferredRevision
  );
}

export function NACECodeBrowser({
  defaultRevision = "2.1",
  onSelect,
}: NACECodeBrowserProps) {
  const [revisions, setRevisions] = useState<NACERevision[]>([]);
  const [revision, setRevision] = useState(defaultRevision);
  const [path, setPath] = useState<NACECode[]>([]);
  const [codes, setCodes] = useState<NACECode[]>([]);
  const [loadingRevisions, setLoadingRevisions] = useState(true);
  const [loadingCodes, setLoadingCodes] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const currentParent = path.at(-1);
  const revisionSummary = useMemo(
    () => revisions.find((item) => item.revision === revision),
    [revision, revisions],
  );

  useEffect(() => {
    let cancelled = false;
    setLoadingRevisions(true);
    setError(null);
    api
      .getNACERevisions()
      .then((response) => {
        if (cancelled) return;
        setRevisions(response.items);
        setRevision(chooseInitialRevision(response.items, defaultRevision));
      })
      .catch((err) => {
        if (!cancelled)
          setError(errorMessage(err, "Failed to load NACE revisions."));
      })
      .finally(() => {
        if (!cancelled) setLoadingRevisions(false);
      });
    return () => {
      cancelled = true;
    };
  }, [defaultRevision]);

  useEffect(() => {
    let cancelled = false;
    setLoadingCodes(true);
    setError(null);
    api
      .getNACECodeChildren({
        revision,
        parent_id: currentParent?.id,
      })
      .then((response) => {
        if (!cancelled) setCodes(response.items);
      })
      .catch((err) => {
        if (!cancelled)
          setError(errorMessage(err, "Failed to load NACE categories."));
      })
      .finally(() => {
        if (!cancelled) setLoadingCodes(false);
      });
    return () => {
      cancelled = true;
    };
  }, [currentParent?.id, revision]);

  function openRoot() {
    setPath([]);
  }

  function openPathIndex(index: number) {
    setPath(path.slice(0, index + 1));
  }

  function chooseCode(code: NACECode) {
    onSelect?.(code);
    if (code.has_children) {
      setPath((current) => [...current, code]);
    }
  }

  function changeRevision(value: string) {
    setRevision(value);
    setPath([]);
  }

  return (
    <div className="flex flex-col gap-4">
      <div className="flex flex-col gap-3 sm:flex-row sm:items-end sm:justify-between">
        <div>
          <h2 className="text-base font-semibold">Browse taxonomy</h2>
          <p className="text-sm text-muted-foreground">
            Drill down from root NACE sections to child divisions, groups, and
            classes.
          </p>
        </div>
        <div className="flex min-w-48 flex-col gap-1">
          <label
            htmlFor="nace-browser-revision"
            className="text-xs font-medium text-muted-foreground"
          >
            Revision
          </label>
          <select
            id="nace-browser-revision"
            className="h-9 rounded-md border border-input bg-background px-3 text-sm"
            disabled={loadingRevisions || revisions.length === 0}
            value={revision}
            onChange={(event) => changeRevision(event.target.value)}
          >
            {revisions.length === 0 ? (
              <option value={revision}>{revision}</option>
            ) : (
              revisions.map((item) => (
                <option key={item.revision} value={item.revision}>
                  {item.revision}
                </option>
              ))
            )}
          </select>
        </div>
      </div>

      {revisionSummary && (
        <div className="flex flex-wrap gap-2 text-sm">
          <Badge variant="outline">{revisionSummary.active_codes} active</Badge>
          <Badge variant="outline">{revisionSummary.sections} sections</Badge>
          <Badge variant="outline">{revisionSummary.divisions} divisions</Badge>
          <Badge variant="outline">{revisionSummary.groups} groups</Badge>
          <Badge variant="outline">{revisionSummary.classes} classes</Badge>
        </div>
      )}

      {error && (
        <Alert variant="destructive">
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      )}

      <div className="flex flex-wrap items-center gap-1 text-sm">
        <Button
          type="button"
          variant={path.length === 0 ? "secondary" : "ghost"}
          size="sm"
          onClick={openRoot}
        >
          Root
        </Button>
        {path.map((item, index) => (
          <div key={item.id} className="flex items-center gap-1">
            <ChevronRight className="size-4 text-muted-foreground" />
            <Button
              type="button"
              variant={index === path.length - 1 ? "secondary" : "ghost"}
              size="sm"
              onClick={() => openPathIndex(index)}
            >
              {item.code}
            </Button>
          </div>
        ))}
      </div>

      <div className="overflow-hidden rounded-md border">
        {loadingCodes ? (
          <div className="flex flex-col gap-3 p-4">
            <div className="flex items-center gap-2 text-sm text-muted-foreground">
              <Loader2 className="size-4 animate-spin" />
              Loading categories
            </div>
            {Array.from({ length: 5 }).map((_, index) => (
              <Skeleton key={index} className="h-12 w-full" />
            ))}
          </div>
        ) : codes.length === 0 ? (
          <div className="p-8 text-center text-sm text-muted-foreground">
            No child categories found.
          </div>
        ) : (
          <div className="divide-y">
            {codes.map((code) => (
              <button
                key={code.id}
                type="button"
                className="grid w-full grid-cols-[7rem_1fr_auto] items-center gap-4 px-4 py-3 text-left transition-colors hover:bg-muted/50"
                onClick={() => chooseCode(code)}
              >
                <div className="flex flex-col gap-1">
                  <span className="font-mono text-sm font-medium">
                    {code.code}
                  </span>
                  <span className="text-xs text-muted-foreground">
                    {levelLabel(code.level_name)}
                  </span>
                </div>
                <div className="min-w-0">
                  <div className="truncate text-sm font-medium">
                    {code.title}
                  </div>
                  {code.description && (
                    <div className="mt-1 line-clamp-2 text-xs leading-5 text-muted-foreground">
                      {code.description}
                    </div>
                  )}
                </div>
                {code.has_children ? (
                  <div className="flex items-center gap-1 text-xs text-muted-foreground">
                    Open
                    <ChevronRight className="size-4" />
                  </div>
                ) : (
                  <Badge variant="outline">Leaf</Badge>
                )}
              </button>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
