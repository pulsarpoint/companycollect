import { useOutletContext } from "react-router";
import type { SourceDetailContext } from "~/routes/sources_.$name";
import { BrregRawRecordsTable } from "~/components/app/BrregRawRecordsTable";
import { RawInputsTable } from "~/components/app/RawInputsTable";

export default function SourceRawInputPage() {
  const { source } = useOutletContext<SourceDetailContext>();
  if (source.name === "brreg") return <BrregRawRecordsTable />;
  return (
    <RawInputsTable
      sourceName={source.name}
      requiresTranslation={source.requires_translation}
    />
  );
}
