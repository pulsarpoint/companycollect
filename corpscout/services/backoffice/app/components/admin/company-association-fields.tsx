import { Field, FieldContent, FieldDescription, FieldGroup, FieldLabel, FieldLegend, FieldSet } from "~/components/ui/field";
import { RadioGroup, RadioGroupItem } from "~/components/ui/radio-group";
import { Switch } from "~/components/ui/switch";

export function CompanyAssociationFields({enabled, onEnabledChange, idPrefix, disabled = false}: {
  enabled: boolean; onEnabledChange: (enabled: boolean) => void; idPrefix: string; disabled?: boolean;
}) {
  const id = (name: string) => `${idPrefix}-${name}`;
  return <FieldGroup className="gap-4 sm:col-span-2">
    <input type="hidden" name="match_company" value={String(enabled)} />
    <Field orientation="horizontal" data-disabled={disabled}>
      <FieldContent>
        <FieldLabel htmlFor={id("company-association")}>Company association</FieldLabel>
        <FieldDescription id={id("company-association-description")}>Find the company operating each website and save a proposal for review.</FieldDescription>
      </FieldContent>
      <Switch id={id("company-association")} checked={enabled} onCheckedChange={onEnabledChange}
        disabled={disabled} aria-describedby={id("company-association-description")} />
    </Field>
    {enabled && <FieldSet className="gap-3">
      <FieldLegend variant="label" id={id("association-scope")}>Domains to analyze</FieldLegend>
      <RadioGroup name="skip_company_matching_if_mapped" defaultValue="true" aria-labelledby={id("association-scope")}>
        <Field orientation="horizontal">
          <RadioGroupItem id={id("association-unlinked")} value="true" />
          <FieldLabel htmlFor={id("association-unlinked")}>Only domains without an associated company</FieldLabel>
        </Field>
        <Field orientation="horizontal">
          <RadioGroupItem id={id("association-all")} value="false" />
          <FieldLabel htmlFor={id("association-all")}>Analyze all domains</FieldLabel>
        </Field>
      </RadioGroup>
      <FieldDescription>The selected crawl still runs for every domain. This choice controls company analysis only.</FieldDescription>
    </FieldSet>}
  </FieldGroup>;
}
