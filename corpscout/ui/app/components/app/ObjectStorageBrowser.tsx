import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useSearchParams } from "react-router";
import {
  ChevronLeft,
  ChevronRight,
  Folder,
  Loader2,
  RefreshCw,
  Search,
  Server,
} from "lucide-react";

import { Alert, AlertDescription } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Input } from "~/components/ui/input";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";
import { api, errorMessage } from "~/lib/api";
import { cn, formatDate } from "~/lib/utils";
import type {
  ObjectStorageBucket,
  ObjectStorageObjectListResponse,
} from "~/types/api";

const objectListLimit = 100;

function formatBytes(value: number): string {
  if (!Number.isFinite(value) || value < 0) return "-";
  if (value === 0) return "0 B";

  const units = ["B", "KB", "MB", "GB", "TB"];
  const exponent = Math.min(
    Math.floor(Math.log(value) / Math.log(1024)),
    units.length - 1,
  );
  const amount = value / 1024 ** exponent;

  return `${amount.toFixed(amount >= 10 || exponent === 0 ? 0 : 1)} ${units[exponent]}`;
}

function formatObjectDate(value: string): string {
  if (!value) return "-";

  try {
    return formatDate(value);
  } catch {
    return value;
  }
}

function prefixBreadcrumbs(prefix: string): Array<{
  label: string;
  prefix: string;
}> {
  if (!prefix) return [];

  const breadcrumbs: Array<{ label: string; prefix: string }> = [];
  let segmentStart = 0;

  while (segmentStart < prefix.length) {
    const delimiterIndex = prefix.indexOf("/", segmentStart);
    if (delimiterIndex === -1) {
      const label = prefix.slice(segmentStart);
      breadcrumbs.push({ label: label || "/", prefix });
      break;
    }

    const label = prefix.slice(segmentStart, delimiterIndex);
    breadcrumbs.push({
      label: label || "/",
      prefix: prefix.slice(0, delimiterIndex + 1),
    });
    segmentStart = delimiterIndex + 1;
  }

  return breadcrumbs;
}

export function ObjectStorageBrowser() {
  const [searchParams, setSearchParams] = useSearchParams();
  const selectedBucket = searchParams.get("bucket") ?? "";
  const selectedPrefix = searchParams.get("prefix") ?? "";

  const [prefixInput, setPrefixInput] = useState(selectedPrefix);
  const [buckets, setBuckets] = useState<ObjectStorageBucket[]>([]);
  const [listing, setListing] =
    useState<ObjectStorageObjectListResponse | null>(null);
  const [currentCursor, setCurrentCursor] = useState("");
  const [previousCursors, setPreviousCursors] = useState<string[]>([]);
  const [loadingBuckets, setLoadingBuckets] = useState(true);
  const [loadingObjects, setLoadingObjects] = useState(false);
  const [bucketError, setBucketError] = useState<string | null>(null);
  const [objectsError, setObjectsError] = useState<string | null>(null);
  const bucketRequestID = useRef(0);
  const objectRequestID = useRef(0);

  const breadcrumbs = useMemo(
    () => prefixBreadcrumbs(selectedPrefix),
    [selectedPrefix],
  );
  const selectedBucketMetadata = useMemo(
    () => buckets.find((bucket) => bucket.name === selectedBucket),
    [buckets, selectedBucket],
  );

  const updateLocation = useCallback(
    (bucket: string, prefix: string) => {
      const currentBucket = searchParams.get("bucket") ?? "";
      const currentPrefix = searchParams.get("prefix") ?? "";
      if (currentBucket === bucket && currentPrefix === prefix) return;

      const next = new URLSearchParams(searchParams);

      if (bucket) next.set("bucket", bucket);
      else next.delete("bucket");

      if (prefix) next.set("prefix", prefix);
      else next.delete("prefix");

      setSearchParams(next);
    },
    [searchParams, setSearchParams],
  );

  const loadBuckets = useCallback(async () => {
    const requestID = bucketRequestID.current + 1;
    bucketRequestID.current = requestID;

    setLoadingBuckets(true);
    setBucketError(null);

    try {
      const response = await api.getObjectStorageBuckets();
      if (bucketRequestID.current !== requestID) return;
      setBuckets(response.items);
    } catch (error) {
      if (bucketRequestID.current !== requestID) return;
      setBucketError(
        errorMessage(error, "Failed to load object storage buckets."),
      );
    } finally {
      if (bucketRequestID.current === requestID) setLoadingBuckets(false);
    }
  }, []);

  const loadObjects = useCallback(
    async (cursor = "") => {
      const requestID = objectRequestID.current + 1;
      objectRequestID.current = requestID;

      if (!selectedBucket) {
        setListing(null);
        setObjectsError(null);
        setLoadingObjects(false);
        return false;
      }

      setLoadingObjects(true);
      setObjectsError(null);

      try {
        const response = await api.getObjectStorageObjects(selectedBucket, {
          prefix: selectedPrefix,
          delimiter: "/",
          cursor,
          limit: objectListLimit,
        });
        if (objectRequestID.current !== requestID) return false;
        setListing(response);
        return true;
      } catch (error) {
        if (objectRequestID.current !== requestID) return false;
        setListing(null);
        setObjectsError(errorMessage(error, "Failed to load bucket contents."));
        return false;
      } finally {
        if (objectRequestID.current === requestID) setLoadingObjects(false);
      }
    },
    [selectedBucket, selectedPrefix],
  );

  useEffect(() => {
    void loadBuckets();
  }, [loadBuckets]);

  useEffect(() => {
    setPrefixInput(selectedPrefix);
    setListing(null);
    setObjectsError(null);
    setCurrentCursor("");
    setPreviousCursors([]);
    void loadObjects("");
  }, [loadObjects, selectedPrefix]);

  function openBucket(bucket: string) {
    updateLocation(bucket, "");
  }

  function openPrefix(prefix: string) {
    if (!selectedBucket) return;
    updateLocation(selectedBucket, prefix);
  }

  function submitPrefix() {
    if (!selectedBucket) return;

    updateLocation(selectedBucket, prefixInput);
  }

  function clearPrefix() {
    setPrefixInput("");
    if (selectedBucket) updateLocation(selectedBucket, "");
  }

  async function refresh() {
    void loadBuckets();
    await loadObjects(currentCursor);
  }

  async function openNextPage() {
    if (!listing?.next_cursor) return;

    const nextCursor = listing.next_cursor;
    const loaded = await loadObjects(nextCursor);
    if (!loaded) return;

    setPreviousCursors((cursors) => [...cursors, currentCursor]);
    setCurrentCursor(nextCursor);
  }

  async function openPreviousPage() {
    const previousCursor = previousCursors.at(-1);
    if (previousCursor == null) return;

    const loaded = await loadObjects(previousCursor);
    if (!loaded) return;

    setPreviousCursors((cursors) => cursors.slice(0, -1));
    setCurrentCursor(previousCursor);
  }

  const hasObjects =
    (listing?.folders.length ?? 0) > 0 || (listing?.objects.length ?? 0) > 0;

  return (
    <div className="flex flex-col gap-5">
      <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
        <div className="flex flex-col gap-1">
          <h1 className="text-xl font-semibold">Object Storage</h1>
          <p className="text-sm text-muted-foreground">
            Browse bucket folders and object metadata without reading object
            bodies.
          </p>
        </div>
        <Button
          type="button"
          variant="outline"
          onClick={() => void refresh()}
          disabled={loadingBuckets || loadingObjects}
        >
          <RefreshCw data-icon="inline-start" />
          Refresh
        </Button>
      </div>

      {bucketError ? (
        <Alert variant="destructive">
          <AlertDescription>{bucketError}</AlertDescription>
        </Alert>
      ) : null}

      <div className="grid gap-4 lg:grid-cols-[18rem_minmax(0,1fr)]">
        <section className="overflow-hidden rounded-md border">
          <div className="flex items-start justify-between gap-3 border-b p-3">
            <div className="flex flex-col gap-1">
              <h2 className="text-sm font-medium">Buckets</h2>
              <p className="text-xs text-muted-foreground">
                {loadingBuckets
                  ? "Loading buckets"
                  : `${buckets.length.toLocaleString()} visible`}
              </p>
            </div>
            {loadingBuckets ? (
              <Loader2 className="size-4 animate-spin text-muted-foreground" />
            ) : null}
          </div>

          <div className="flex flex-col">
            {buckets.map((bucket) => (
              <Button
                key={bucket.name}
                type="button"
                variant={selectedBucket === bucket.name ? "secondary" : "ghost"}
                size="sm"
                className={cn(
                  "h-auto w-full justify-start rounded-none border-b px-3 py-2",
                  selectedBucket === bucket.name && "font-medium",
                )}
                onClick={() => openBucket(bucket.name)}
              >
                <Server
                  data-icon="inline-start"
                  className="text-muted-foreground"
                />
                <span className="min-w-0 flex-1 truncate text-left">
                  {bucket.name}
                </span>
              </Button>
            ))}

            {!loadingBuckets && buckets.length === 0 ? (
              <div className="p-4 text-sm text-muted-foreground">
                No buckets visible to the configured credentials.
              </div>
            ) : null}
          </div>
        </section>

        <section className="min-w-0 overflow-hidden rounded-md border">
          <div className="flex flex-col gap-3 border-b p-3">
            <div className="flex flex-col gap-2 lg:flex-row lg:items-start lg:justify-between">
              <div className="min-w-0">
                <h2 className="text-sm font-medium">
                  {selectedBucket || "Bucket contents"}
                </h2>
                {selectedBucketMetadata?.creation_date ? (
                  <p className="text-xs text-muted-foreground">
                    Created{" "}
                    {formatObjectDate(selectedBucketMetadata.creation_date)}
                  </p>
                ) : (
                  <p className="text-xs text-muted-foreground">
                    {selectedBucket
                      ? "Folder-like prefixes and object metadata"
                      : "Select a bucket to begin"}
                  </p>
                )}
              </div>
              {listing ? (
                <div className="flex flex-wrap gap-2">
                  <Badge variant="outline">
                    {listing.folders.length.toLocaleString()} folders
                  </Badge>
                  <Badge variant="outline">
                    {listing.objects.length.toLocaleString()} objects
                  </Badge>
                </div>
              ) : null}
            </div>

            <div className="flex flex-wrap items-center gap-1 text-sm">
              {selectedBucket ? (
                <>
                  <Button
                    type="button"
                    variant={selectedPrefix ? "ghost" : "secondary"}
                    size="sm"
                    onClick={() => openPrefix("")}
                  >
                    {selectedBucket}
                  </Button>
                  {breadcrumbs.map((breadcrumb, index) => (
                    <span
                      key={breadcrumb.prefix}
                      className="flex min-w-0 items-center gap-1"
                    >
                      <ChevronRight className="size-4 shrink-0 text-muted-foreground" />
                      <Button
                        type="button"
                        variant={
                          index === breadcrumbs.length - 1
                            ? "secondary"
                            : "ghost"
                        }
                        size="sm"
                        className="max-w-56 truncate"
                        onClick={() => openPrefix(breadcrumb.prefix)}
                      >
                        {breadcrumb.label}
                      </Button>
                    </span>
                  ))}
                </>
              ) : (
                <span className="text-muted-foreground">Select a bucket</span>
              )}
            </div>

            <div className="flex flex-col gap-2 sm:flex-row">
              <div className="relative min-w-0 flex-1">
                <Search className="absolute left-2 top-1/2 size-4 -translate-y-1/2 text-muted-foreground" />
                <Input
                  value={prefixInput}
                  aria-label="Prefix"
                  onChange={(event) => setPrefixInput(event.target.value)}
                  onKeyDown={(event) => {
                    if (event.key === "Enter") submitPrefix();
                  }}
                  placeholder="Prefix, for example brreg/search/"
                  className="pl-8 font-mono text-sm"
                  disabled={!selectedBucket}
                />
              </div>
              <Button
                type="button"
                onClick={submitPrefix}
                disabled={!selectedBucket}
              >
                Open prefix
              </Button>
              <Button
                type="button"
                variant="outline"
                onClick={clearPrefix}
                disabled={!selectedBucket || (!selectedPrefix && !prefixInput)}
              >
                Clear prefix
              </Button>
            </div>
          </div>

          {objectsError ? (
            <Alert variant="destructive" className="m-3">
              <AlertDescription>{objectsError}</AlertDescription>
            </Alert>
          ) : null}

          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Name</TableHead>
                <TableHead>Key / prefix</TableHead>
                <TableHead>Size</TableHead>
                <TableHead>Modified</TableHead>
                <TableHead>ETag</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {!selectedBucket ? (
                <TableRow>
                  <TableCell
                    colSpan={5}
                    className="h-24 text-center text-muted-foreground"
                  >
                    Select a bucket to browse object metadata.
                  </TableCell>
                </TableRow>
              ) : loadingObjects && !listing ? (
                <TableRow>
                  <TableCell
                    colSpan={5}
                    className="h-24 text-center text-muted-foreground"
                  >
                    <span className="inline-flex items-center gap-2">
                      <Loader2 className="size-4 animate-spin" />
                      Loading objects
                    </span>
                  </TableCell>
                </TableRow>
              ) : listing && !hasObjects ? (
                <TableRow>
                  <TableCell
                    colSpan={5}
                    className="h-24 text-center text-muted-foreground"
                  >
                    This prefix is empty.
                  </TableCell>
                </TableRow>
              ) : null}

              {listing?.folders.map((folder) => (
                <TableRow key={`folder:${folder.prefix}`}>
                  <TableCell>
                    <Button
                      type="button"
                      variant="ghost"
                      size="sm"
                      className="max-w-72 justify-start px-2"
                      onClick={() => openPrefix(folder.prefix)}
                    >
                      <Folder
                        data-icon="inline-start"
                        className="text-muted-foreground"
                      />
                      <span className="truncate">
                        {folder.name || folder.prefix}
                      </span>
                    </Button>
                  </TableCell>
                  <TableCell className="max-w-[32rem] truncate font-mono text-xs">
                    {folder.prefix}
                  </TableCell>
                  <TableCell>
                    <Badge variant="outline">Folder</Badge>
                  </TableCell>
                  <TableCell>-</TableCell>
                  <TableCell>-</TableCell>
                </TableRow>
              ))}

              {listing?.objects.map((object) => (
                <TableRow key={`object:${object.key}`}>
                  <TableCell className="max-w-72 truncate font-medium">
                    {object.name || object.key}
                  </TableCell>
                  <TableCell className="max-w-[32rem] truncate font-mono text-xs">
                    {object.key}
                  </TableCell>
                  <TableCell>{formatBytes(object.size_bytes)}</TableCell>
                  <TableCell>{formatObjectDate(object.last_modified)}</TableCell>
                  <TableCell className="max-w-48 truncate font-mono text-xs">
                    {object.etag || "-"}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>

          {selectedBucket &&
          (previousCursors.length > 0 || listing?.next_cursor) ? (
            <div className="flex items-center justify-end gap-2 border-t p-3">
              <Button
                type="button"
                variant="outline"
                onClick={() => void openPreviousPage()}
                disabled={loadingObjects || previousCursors.length === 0}
              >
                <ChevronLeft data-icon="inline-start" />
                Previous page
              </Button>
              {listing?.next_cursor ? (
                <Button
                  type="button"
                  variant="outline"
                  onClick={() => void openNextPage()}
                  disabled={loadingObjects}
                >
                  Next page
                  <ChevronRight data-icon="inline-end" />
                </Button>
              ) : null}
            </div>
          ) : null}
        </section>
      </div>
    </div>
  );
}
