import { useOutletContext } from "react-router";
import type { SourceDetailContext } from "~/routes/sources_.$name";
import { PipelineTab } from "~/components/app/source-detail/PipelineTab";
import { Alert, AlertDescription } from "~/components/ui/alert";
import { hasPipeline } from "~/components/app/source-detail/sourceDetailUtils";

export default function SourcePipelinePage() {
  const { source } = useOutletContext<SourceDetailContext>();
  if (!hasPipeline(source)) {
    return (
      <Alert>
        <AlertDescription>Pipeline sync is not available for this source.</AlertDescription>
      </Alert>
    );
  }
  return <PipelineTab source={source} />;
}
