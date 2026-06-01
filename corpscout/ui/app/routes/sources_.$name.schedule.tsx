import { useOutletContext } from "react-router";
import type { SourceDetailContext } from "~/routes/sources_.$name";
import { ScheduleTab } from "~/components/app/source-detail/ScheduleTab";

export default function SourceSchedulePage() {
  const { source, saving, triggering, onPatch, onTrigger } =
    useOutletContext<SourceDetailContext>();
  return (
    <ScheduleTab
      source={source}
      saving={saving}
      triggering={triggering}
      onPatch={onPatch}
      onTrigger={onTrigger}
    />
  );
}
