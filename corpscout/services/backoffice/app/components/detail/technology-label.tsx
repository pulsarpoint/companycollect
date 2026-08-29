import type { TechnologyCatalogEntry } from "~/lib/technology-catalog.server";
import { cn } from "~/lib/utils";

/**
 * A technology name with its catalog icon. Icons are served through the
 * /icons/tech/:slug proxy; when the catalog has no icon (or no entry at all)
 * a monogram block with the technology's initial stands in, so lists stay
 * visually aligned. The catalog description rides along as a `title` hover.
 */

export function TechnologyIcon({
  name,
  entry,
}: {
  name: string;
  entry?: TechnologyCatalogEntry;
}) {
  if (entry?.icon) {
    return (
      <img
        src={`/icons/tech/${entry.slug}`}
        alt=""
        loading="lazy"
        width={16}
        height={16}
        className="size-4 shrink-0 rounded-[3px] object-contain"
      />
    );
  }
  return (
    <span
      aria-hidden
      data-slot="technology-monogram"
      className="bg-muted text-muted-foreground flex size-4 shrink-0 items-center justify-center rounded-[3px] text-[9px] font-semibold"
    >
      {(name.trim()[0] ?? "?").toUpperCase()}
    </span>
  );
}

export function TechnologyLabel({
  name,
  entry,
  className,
}: {
  name: string;
  entry?: TechnologyCatalogEntry;
  className?: string;
}) {
  return (
    <span
      className={cn("inline-flex items-center gap-1.5", className)}
      title={entry?.description || undefined}
    >
      <TechnologyIcon name={name} entry={entry} />
      <span>{name}</span>
    </span>
  );
}
