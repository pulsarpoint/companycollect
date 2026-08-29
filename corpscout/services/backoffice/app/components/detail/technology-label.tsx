import { Link } from "react-router";
// Type-only: erased at build, keeps the ClickHouse module out of the client
// bundle (see backoffice/CLAUDE.md on `.server` imports in components).
import type { TechnologyCatalogEntry } from "~/lib/technology-catalog.server";
import { technologyDetailPath } from "~/lib/technologies";
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
  linkToCatalog = false,
}: {
  name: string;
  entry?: TechnologyCatalogEntry;
  className?: string;
  /**
   * ADMIN pages only: link the name to /admin/technologies/:slug when the
   * enrichment map knows the catalog slug. Public pages keep the default
   * plain label -- they must never point into the admin area.
   */
  linkToCatalog?: boolean;
}) {
  return (
    <span
      className={cn("inline-flex items-center gap-1.5", className)}
      title={entry?.description || undefined}
    >
      <TechnologyIcon name={name} entry={entry} />
      {linkToCatalog && entry?.slug ? (
        <Link
          to={technologyDetailPath(entry.slug)}
          className="underline-offset-2 hover:underline"
        >
          {name}
        </Link>
      ) : (
        <span>{name}</span>
      )}
    </span>
  );
}
