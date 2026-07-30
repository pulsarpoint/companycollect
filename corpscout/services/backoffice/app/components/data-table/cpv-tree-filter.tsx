import { useEffect, useState } from "react";
import { ChevronDown, ChevronRight, Loader2 } from "lucide-react";

import type { CpvTreeNode } from "~/lib/contracts.server";
import { Checkbox } from "~/components/ui/checkbox";

const nf = new Intl.NumberFormat("en-US");

/**
 * The CPV classification as a tree you drill into.
 *
 * CPV is hierarchical and selection follows the hierarchy: ticking a node
 * selects it and everything beneath it, because a node's significant prefix is
 * shared by every descendant. So "Construction work" returns all 7,323
 * Estonian construction contracts without a reader having to know that
 * 45213100 is one of them.
 *
 * Levels load on demand. A country's used vocabulary is 1,404 nodes (Norway) to
 * 3,353 (Sweden) — a quarter of a megabyte of labels on every page load for a
 * panel most readers never open. The 45 divisions arrive with the page and each
 * expansion fetches one more level.
 *
 * A parent already selected disables its descendants rather than hiding them:
 * they ARE included, and letting someone tick a child that changes nothing
 * would be a control that lies.
 */
export function CpvTreeFilter({
  countryCode,
  roots,
  selected,
  onToggle,
}: {
  countryCode: string;
  roots: CpvTreeNode[];
  selected: string[];
  onToggle: (prefix: string) => void;
}) {
  return (
    <div className="flex max-h-96 flex-col gap-0.5 overflow-y-auto pr-1">
      {roots.map((node) => (
        <CpvNode
          key={node.code}
          countryCode={countryCode}
          node={node}
          depth={0}
          selected={selected}
          onToggle={onToggle}
        />
      ))}
    </div>
  );
}

function CpvNode({
  countryCode,
  node,
  depth,
  selected,
  onToggle,
}: {
  countryCode: string;
  node: CpvTreeNode;
  depth: number;
  selected: string[];
  onToggle: (prefix: string) => void;
}) {
  const [open, setOpen] = useState(false);
  const [children, setChildren] = useState<CpvTreeNode[] | null>(null);
  const [loading, setLoading] = useState(false);

  // An ancestor being selected already includes this node.
  const coveredByAncestor = selected.some(
    (p) => p !== node.prefix && node.prefix.startsWith(p),
  );
  const checked = selected.includes(node.prefix) || coveredByAncestor;

  useEffect(() => {
    if (!open || children !== null || loading) return;
    let cancelled = false;
    setLoading(true);
    fetch(
      `/countries/${countryCode}/contracts-cpv?parent=${encodeURIComponent(node.code)}`,
    )
      .then((response) => (response.ok ? response.json() : { nodes: [] }))
      .then((data) => {
        if (!cancelled) setChildren(data.nodes ?? []);
      })
      // An empty list rather than a stuck spinner: the panel keeps working and
      // the node simply shows nothing beneath it.
      .catch(() => {
        if (!cancelled) setChildren([]);
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [open, children, loading, countryCode, node.code]);

  return (
    <div>
      <div
        className="flex items-start gap-1.5 text-sm"
        style={{ paddingLeft: `${depth * 0.9}rem` }}
      >
        {node.hasChildren ? (
          <button
            type="button"
            onClick={() => setOpen((v) => !v)}
            className="text-muted-foreground hover:text-foreground mt-0.5 shrink-0"
            aria-label={open ? `Collapse ${node.label}` : `Expand ${node.label}`}
            aria-expanded={open}
          >
            {loading ? (
              <Loader2 className="size-3.5 animate-spin" />
            ) : open ? (
              <ChevronDown className="size-3.5" />
            ) : (
              <ChevronRight className="size-3.5" />
            )}
          </button>
        ) : (
          <span className="size-3.5 shrink-0" />
        )}
        <label
          className={`flex flex-1 items-start gap-2 ${
            coveredByAncestor ? "cursor-default" : "cursor-pointer"
          }`}
        >
          <Checkbox
            checked={checked}
            disabled={coveredByAncestor}
            onCheckedChange={() => {
              if (!coveredByAncestor) onToggle(node.prefix);
            }}
            className="mt-0.5"
          />
          <span className="flex-1 leading-snug">
            {node.label}
            <span className="text-muted-foreground/70 ml-1.5 font-mono text-[10px]">
              {node.prefix}
            </span>
          </span>
          <span className="text-muted-foreground shrink-0 text-xs tabular-nums">
            {nf.format(node.contracts)}
          </span>
        </label>
      </div>
      {open && children !== null
        ? children.map((child) => (
            <CpvNode
              key={child.code}
              countryCode={countryCode}
              node={child}
              depth={depth + 1}
              selected={selected}
              onToggle={onToggle}
            />
          ))
        : null}
    </div>
  );
}
