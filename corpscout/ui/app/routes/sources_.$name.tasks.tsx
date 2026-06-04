import { useOutletContext } from "react-router";

import { BrregTaskStateTab } from "~/components/app/source-detail/BrregTaskStateTab";
import { Alert, AlertDescription } from "~/components/ui/alert";
import type { SourceDetailContext } from "~/routes/sources_.$name";

export default function SourceBrregTaskStatePage() {
  const { source } = useOutletContext<SourceDetailContext>();
  if (source.name === "brreg") return <BrregTaskStateTab />;
  if (source.name === "france") {
    return (
      <Alert>
        <AlertDescription>
          France task state will be available after normalized SIRENE processing is added. Use Raw Inputs to inspect ingested legal units and establishments.
        </AlertDescription>
      </Alert>
    );
  }

  return (
    <Alert>
      <AlertDescription>Task state is not available for this source.</AlertDescription>
    </Alert>
  );
}
