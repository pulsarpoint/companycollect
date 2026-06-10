import { useOutletContext } from "react-router";

import { ActionsTab } from "~/components/app/source-detail/ActionsTab";
import type { SourceDetailContext } from "~/routes/sources_.$name";

export default function SourceActionsPage() {
  const { source } = useOutletContext<SourceDetailContext>();
  return <ActionsTab source={source} />;
}
