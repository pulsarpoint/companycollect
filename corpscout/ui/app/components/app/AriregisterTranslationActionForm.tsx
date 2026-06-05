import { BrregTranslationActionForm } from "~/components/app/BrregTranslationActionForm";
import { api } from "~/lib/api";

type Props = Omit<
  Parameters<typeof BrregTranslationActionForm>[0],
  | "description"
  | "failureMessage"
  | "formIdPrefix"
  | "sourceDatabase"
  | "sourceLabel"
  | "submitTranslation"
  | "successMessage"
>;

export function AriregisterTranslationActionForm(props: Props) {
  return (
    <BrregTranslationActionForm
      {...props}
      sourceLabel="Ariregister"
      sourceDatabase="ariregister_source"
      formIdPrefix="ariregister"
      description="Starts the Temporal workflow that prepares a Postgres Ariregister company translation queue, translates uncached Estonian terms, and writes English values back to ariregister_source."
      submitTranslation={api.translateAriregisterSourceCompanies}
      successMessage="Ariregister company translation workflow started."
      failureMessage="Failed to start Ariregister translation."
    />
  );
}
