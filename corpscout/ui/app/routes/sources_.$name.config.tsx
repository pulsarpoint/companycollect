import { useOutletContext } from "react-router";
import type { SourceDetailContext } from "~/routes/sources_.$name";
import { ConfigTab } from "~/components/app/source-detail/ConfigTab";

export default function SourceConfigPage() {
  const { source } = useOutletContext<SourceDetailContext>();
  return <ConfigTab source={source} />;
}
