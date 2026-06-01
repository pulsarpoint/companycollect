import { useOutletContext } from "react-router";
import type { SourceDetailContext } from "~/routes/sources_.$name";
import { ConfigTab } from "~/components/app/source-detail/ConfigTab";

export default function SourceConfigPage() {
  const { source, saving, onPatch } = useOutletContext<SourceDetailContext>();
  return <ConfigTab source={source} saving={saving} onPatch={onPatch} />;
}
