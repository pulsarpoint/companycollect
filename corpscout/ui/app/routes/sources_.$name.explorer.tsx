import { useOutletContext } from "react-router";

import { FinlandPRHYTJExplorerTab } from "~/components/app/source-detail/FinlandPRHYTJExplorerTab";
import type { SourceDetailContext } from "~/routes/sources_.$name";
import { Alert, AlertDescription } from "~/components/ui/alert";
import { hasCompanyExplorer } from "~/components/app/source-detail/sourceDetailUtils";

export default function SourceExplorerPage() {
  const { source } = useOutletContext<SourceDetailContext>();
  if (!hasCompanyExplorer(source)) {
    return (
      <Alert>
        <AlertDescription>Company explorer is not available for this source.</AlertDescription>
      </Alert>
    );
  }
  return <FinlandPRHYTJExplorerTab source={source} />;
}
