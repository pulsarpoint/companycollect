import { useOutletContext } from "react-router";
import type { SourceDetailContext } from "~/routes/sources_.$name";
import { ScheduleTab } from "~/components/app/source-detail/ScheduleTab";

export default function SourceSchedulePage() {
  const { source } = useOutletContext<SourceDetailContext>();
  return <ScheduleTab source={source} />;
}
