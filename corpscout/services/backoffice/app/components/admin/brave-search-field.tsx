import { useEffect, useRef, useState } from "react";
import { Link, useFetcher } from "react-router";
import type { loader } from "~/routes/admin-brave-search-options";
import { braveSearchPreview } from "~/lib/brave-searches";
import { Button } from "~/components/ui/button";
import { Field, FieldDescription, FieldLabel } from "~/components/ui/field";
import { NativeSelect, NativeSelectOption } from "~/components/ui/native-select";

export function BraveSearchField({taskId}: {taskId: string}) {
  const options = useFetcher<typeof loader>();
  const requested = useRef(false);
  const [searchId, setSearchId] = useState("");
  const path = `/admin/brave/search-options?${new URLSearchParams({task: taskId})}`;
  useEffect(() => { if (!requested.current) { requested.current = true; void options.load(path); } }, [options.load, path]);
  const frozen = options.data?.frozen;
  const selected = options.data?.searches.find(search => search.searchId === searchId);
  const loading = !options.data || options.state !== "idle";
  const template = frozen?.query_template ?? selected?.queryTemplate ?? "";
  return <Field className="sm:col-span-2">
    <FieldLabel htmlFor="queue-brave-search">Brave search</FieldLabel>
    {frozen ? <>
      <input type="hidden" name="brave_search_id" value="saved" />
      <p id="queue-brave-search" className="text-sm font-medium">{frozen.search_name ?? "Saved search"} · {frozen.search_revision ? `version ${frozen.search_revision}` : "original question"}</p>
      <FieldDescription>This task has started and will resume its original question. Add inputs to a new queue to use a different search.</FieldDescription>
    </> : <NativeSelect id="queue-brave-search" name="brave_search_id" value={selected?.searchId ?? ""} onChange={event => setSearchId(event.target.value)} required>
      <NativeSelectOption value="">{loading ? "Loading searches…" : "Choose a Brave search"}</NativeSelectOption>
      {options.data?.searches.map(search => <NativeSelectOption key={search.searchId} value={search.searchId}>{search.name} · version {search.revision}</NativeSelectOption>)}
    </NativeSelect>}
    <input type="hidden" name="brave_search_revision" value={frozen?.search_revision ?? selected?.revision ?? 0} />
    {template && <div className="flex flex-col gap-2 text-sm"><p className="whitespace-pre-wrap break-words">{template}</p><p className="whitespace-pre-wrap break-words text-muted-foreground">Example: {braveSearchPreview(template)}</p></div>}
    {options.data?.error && <p role="alert" className="text-sm text-destructive">{options.data.error}</p>}
    {!loading && !frozen && !options.data?.searches.length && !options.data?.error && <FieldDescription>Add a search in settings before starting processing.</FieldDescription>}
    <div className="flex items-center gap-3"><Link className="text-sm underline" to="/admin/settings/brave-searches" target="_blank" rel="noreferrer">Manage Brave searches</Link>
      {options.data?.error && <Button type="button" variant="outline" size="sm" onClick={() => options.load(path)}>Retry</Button>}
    </div>
  </Field>;
}
