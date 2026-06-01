import { useOutletContext } from "react-router";

import { BrregTaskStateTab } from "~/components/app/source-detail/BrregTaskStateTab";
import { Alert, AlertDescription } from "~/components/ui/alert";
import type { SourceDetailContext } from "~/routes/sources_.$name";

export default function SourceBrregTaskStatePage() {
  const { source } = useOutletContext<SourceDetailContext>();
  if (source.name !== "brreg") {
    return (
      <Alert>
        <AlertDescription>Task state is available for BRREG only.</AlertDescription>
      </Alert>
    );
  }

  return <BrregTaskStateTab />;
}
