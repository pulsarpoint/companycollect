import { useOutletContext } from "react-router";
import type { SourceDetailContext } from "~/routes/sources_.$name";
import { Alert, AlertDescription } from "~/components/ui/alert";
import { AriregisterSourceEntriesTable } from "~/components/app/AriregisterSourceEntriesTable";
import { BrregSourceEntriesTable } from "~/components/app/BrregSourceEntriesTable";

export default function SourceEntriesPage() {
  const { source } = useOutletContext<SourceDetailContext>();
  if (source.name === "brreg") return <BrregSourceEntriesTable />;
  if (source.name === "ariregister") return <AriregisterSourceEntriesTable />;
  if (source.name === "france") {
    return (
      <Alert>
        <AlertDescription>
          France source entries will be available after raw SIRENE records are normalized into the france_source schema.
        </AlertDescription>
      </Alert>
    );
  }
  return (
    <Alert>
      <AlertDescription>Source entries are not available for this source.</AlertDescription>
    </Alert>
  );
}
