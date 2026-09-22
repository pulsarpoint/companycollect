import { useState } from "react";
import { Form, Link } from "react-router";
import { ListFilterIcon, XIcon } from "lucide-react";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import {
  Sheet, SheetContent, SheetDescription, SheetFooter,
  SheetHeader, SheetTitle, SheetTrigger,
} from "~/components/ui/sheet";

/** The button, its active-filter count, and one removable chip per applied
 * filter. Each chip's X re-navigates to the same page without that one param
 * (built from the filter state, never from the live location, so a pending
 * navigation cannot make a chip point at a URL that re-adds it). */
export function ListFilterSheet({
  chips,
  clearHref,
  hrefWithout,
  title,
  description,
  children,
}: {
  chips: { param: string; label: string }[];
  clearHref: string;
  hrefWithout: (param: string) => string;
  title: string;
  description: string;
  children: React.ReactNode;
}) {
  const [open, setOpen] = useState(false);
  return (
    <div className="flex flex-wrap items-center gap-2">
      <Sheet open={open} onOpenChange={setOpen}>
        <SheetTrigger render={<Button variant="outline" size="sm" />}>
          <ListFilterIcon data-icon="inline-start" />
          Filters
          {chips.length > 0 ? (
            <Badge variant="secondary" className="ml-1 px-1.5">
              {chips.length}
            </Badge>
          ) : null}
        </SheetTrigger>
        <SheetContent side="right" className="flex w-full flex-col sm:max-w-sm">
          <SheetHeader>
            <SheetTitle>{title}</SheetTitle>
            <SheetDescription>{description}</SheetDescription>
          </SheetHeader>
          {/* Applying navigates, which leaves this component mounted -- close
              the sheet on submit so the reviewer sees the filtered table. */}
          <Form
            key={JSON.stringify(chips)}
            method="get"
            className="flex min-h-0 flex-1 flex-col gap-4 overflow-y-auto"
            onSubmit={() => setOpen(false)}
          >
            {children}
            <SheetFooter className="flex-row border-t">
              <Button type="submit" className="flex-1">Apply</Button>
              <Button
                variant="outline"
                nativeButton={false}
                render={<Link to={clearHref} />}
                onClick={() => setOpen(false)}
              >
                Clear
              </Button>
            </SheetFooter>
          </Form>
        </SheetContent>
      </Sheet>
      {chips.map((chip) => (
        <Badge key={chip.param} variant="secondary" className="gap-1 pr-1">
          {chip.label}
          <Link
            to={hrefWithout(chip.param)}
            aria-label={`Remove filter ${chip.label}`}
            className="rounded-sm opacity-70 hover:opacity-100"
          >
            <XIcon className="size-3" />
          </Link>
        </Badge>
      ))}
      {chips.length > 0 ? (
        <Button variant="ghost" size="sm" nativeButton={false} render={<Link to={clearHref} />}>
          Clear all
        </Button>
      ) : null}
    </div>
  );
}

