import { Link } from "react-router";
import type { Lang } from "~/components/detail/language";
import { useEffectiveSearchParams } from "~/components/data-table/use-effective-search";
import { Button } from "~/components/ui/button";

const OPTIONS: { value: Lang; label: string }[] = [
  { value: "en", label: "English" },
  { value: "original", label: "Original" },
];

/** Writes `lang=original`, or removes `lang` for the "en" default, preserving every other param. */
function langHref(current: URLSearchParams, lang: Lang): string {
  const next = new URLSearchParams(current);
  if (lang === "original") next.set("lang", "original");
  else next.delete("lang");
  return `?${next.toString()}`;
}

/**
 * Two-option "English" / "Original" segmented control for the detail page
 * header. Hidden entirely when the record has no `_en`/`_original` pairs to
 * switch between (`pairCount === 0`), since there'd be nothing to toggle.
 */
export function LangToggle({ lang, pairCount }: { lang: Lang; pairCount: number }) {
  const searchParams = useEffectiveSearchParams();
  if (pairCount === 0) return null;

  return (
    <div
      role="group"
      aria-label="Record language"
      className="bg-muted inline-flex h-8 items-center gap-0.5 rounded-lg p-[3px]"
    >
      {OPTIONS.map((option) => {
        const active = lang === option.value;
        return (
          <Button
            key={option.value}
            variant="ghost"
            size="sm"
            className="text-foreground/60 hover:text-foreground h-full rounded-md px-2.5 text-xs font-medium hover:bg-transparent data-[active=true]:bg-background data-[active=true]:text-foreground data-[active=true]:shadow-sm dark:data-[active=true]:bg-input/30"
            data-active={active}
            aria-current={active ? "true" : undefined}
            render={<Link to={langHref(searchParams, option.value)} preventScrollReset />}
            nativeButton={false}
          >
            {option.label}
          </Button>
        );
      })}
    </div>
  );
}
