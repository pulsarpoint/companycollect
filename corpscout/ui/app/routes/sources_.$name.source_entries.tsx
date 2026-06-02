import { useOutletContext } from "react-router";
import type { SourceDetailContext } from "~/routes/sources_.$name";
import { Alert, AlertDescription } from "~/components/ui/alert";
import { BrregSourceEntriesTable } from "~/components/app/BrregSourceEntriesTable";

export default function SourceEntriesPage() {
  const { source } = useOutletContext<SourceDetailContext>();
  if (source.name !== "brreg") {
    return (
      <Alert>
        <AlertDescription>Source entries are available for BRREG only.</AlertDescription>
      </Alert>
    );
  }
  return <BrregSourceEntriesTable />;
}
