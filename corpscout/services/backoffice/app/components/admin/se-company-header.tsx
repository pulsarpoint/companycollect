import { Link, NavLink } from "react-router";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { InfoIcon } from "lucide-react";
import { Badge } from "~/components/ui/badge";
import { Tabs, TabsList, TabsTrigger } from "~/components/ui/tabs";
import type { SeCompanyShell } from "~/lib/se-company-shell.server";
import {
  SE_COMPANY_TABS,
  seCompanyTabPath,
  type SeCompanyTab,
} from "~/lib/se-company-tabs";

/**
 * The company area's one header: who this company is, where else to look at
 * it, and the sub-menu every tab renders under.
 *
 * The tabs are `NavLink`s wearing the shadcn Tabs skin (the same trick as
 * the public company layout): the active tab is a route, so it must be
 * navigable, linkable and correct on a cold load -- not a client-side
 * selection. `value={tab}` comes from the URL for exactly that reason.
 */
export function SeCompanyHeader({
  shell,
  tab,
}: {
  shell: SeCompanyShell;
  tab: SeCompanyTab;
}) {
  return (
    <header className="flex flex-col gap-3">
      <div className="flex flex-wrap items-center gap-2">
        <Badge variant="secondary">{shell.status}</Badge>
        {shell.legal_form_code ? (
          // The official Swedish name with the English gloss beside it, the
          // code as the tooltip -- the code alone says nothing, since
          // legal_form_code mixes Bolagsverket text codes with SCB numbers.
          <Badge variant="secondary" title={shell.legal_form_code}>
            {shell.legal_form_label_sv === ""
              ? shell.legal_form_code
              : shell.legal_form_label_sv}
            {shell.legal_form_label_en === "" ? null : (
              <span className="text-muted-foreground font-normal">
                {shell.legal_form_label_en}
              </span>
            )}
          </Badge>
        ) : null}
        {shell.entity_type_label ? (
          <Badge variant={shell.is_public_sector ? "secondary" : "outline"}>
            {shell.entity_type_label}
          </Badge>
        ) : null}
      </div>
      <h1 className="text-2xl font-semibold tracking-tight">
        {shell.legal_name}
      </h1>
      <p className="text-sm text-muted-foreground">
        Company <span className="font-mono">{shell.company_id}</span>
        {shell.incorporation_date === ""
          ? null
          : ` · registered ${shell.incorporation_date}`}
      </p>
      <div className="flex flex-wrap items-center gap-3 text-sm">
        <Link
          className="underline underline-offset-2"
          to={`/company/se/${encodeURIComponent(shell.company_id)}`}
        >
          Company page
        </Link>
      </div>

      {shell.published ? null : (
        <Alert>
          <InfoIcon />
          <AlertTitle>Not published yet</AlertTitle>
          <AlertDescription>
            This company is in the Bolagsverket register but has no
            se_company_info row yet, so the header above is the register's own
            wording. Dagster publishes it once its enrichment run completes.
          </AlertDescription>
        </Alert>
      )}

      <Tabs value={tab}>
        <TabsList variant="line">
          {SE_COMPANY_TABS.map((entry) => (
            <TabsTrigger
              key={entry.value}
              value={entry.value}
              render={<NavLink to={seCompanyTabPath(shell.company_id, entry.value)} />}
              nativeButton={false}
            >
              {entry.label}
            </TabsTrigger>
          ))}
        </TabsList>
      </Tabs>
    </header>
  );
}
