import { Link } from "react-router";
import { ArrowLeft } from "lucide-react";
import { NACECodeBrowser } from "~/components/app/NACECodeBrowser";
import { Button } from "~/components/ui/button";

export default function NACETaxonomyBrowserRoute() {
  return (
    <div className="flex flex-col gap-5">
      <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h1 className="text-xl font-semibold">NACE Browser</h1>
          <p className="text-sm text-muted-foreground">
            Explore the NACE hierarchy from root sections to child divisions,
            groups, and classes.
          </p>
        </div>
        <Button variant="outline" asChild>
          <Link to="/settings/nace-taxonomy">
            <ArrowLeft className="size-4" />
            NACE sync
          </Link>
        </Button>
      </div>

      <section className="rounded-md border p-4">
        <NACECodeBrowser />
      </section>
    </div>
  );
}
