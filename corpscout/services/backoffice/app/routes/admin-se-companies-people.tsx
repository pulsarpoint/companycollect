import { UsersRoundIcon } from "lucide-react";
import {
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "~/components/ui/empty";

// A scaffolded tab: no loader yet, so nothing here imports `~/lib/*.server` and
// the module stays trivially client-safe. The real list (companies by their
// people, with filters and bulk actions) lands later.

export function meta() {
  return [{ title: "Companies · People | CompanyCollect" }];
}

export default function AdminSeCompaniesPeople() {
  return (
    <Empty className="border">
      <EmptyHeader>
        <EmptyMedia variant="icon">
          <UsersRoundIcon />
        </EmptyMedia>
        <EmptyTitle>Coming soon</EmptyTitle>
        <EmptyDescription>
          This view will list companies by their people, with filters and bulk
          actions.
        </EmptyDescription>
      </EmptyHeader>
    </Empty>
  );
}
