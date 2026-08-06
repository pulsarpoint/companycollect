import type {
  EsefDisclosureDocument,
  EsefDisclosureTableCell,
} from "~/lib/esef-disclosures";

function DisclosureRows({
  rows,
  header = false,
}: {
  rows: EsefDisclosureTableCell[][];
  header?: boolean;
}) {
  const Cell = header ? "th" : "td";
  return rows.map((row, rowIndex) => (
    <tr key={rowIndex} className="border-b last:border-b-0">
      {row.map((cell, cellIndex) => (
        <Cell
          key={cellIndex}
          colSpan={cell.colSpan}
          rowSpan={cell.rowSpan}
          scope={header ? "col" : undefined}
          className={
            header
              ? "min-w-24 border-r bg-muted/65 px-3 py-2 text-left align-bottom text-xs font-medium whitespace-nowrap last:border-r-0"
              : "min-w-24 border-r px-3 py-2 text-left align-top text-xs whitespace-nowrap tabular-nums last:border-r-0"
          }
        >
          {cell.text}
        </Cell>
      ))}
    </tr>
  ));
}

export function EsefDisclosureReader({
  disclosure,
}: {
  disclosure: EsefDisclosureDocument;
}) {
  if (disclosure.blocks.length === 0) {
    return (
      <p className="text-muted-foreground">No disclosure text reported.</p>
    );
  }

  return (
    <div className="flex w-full min-w-0 max-w-full flex-col gap-5">
      {disclosure.blocks.map((block, index) => {
        if (block.type !== "table") {
          return block.type === "heading" ? (
            <h5
              key={index}
              className="pt-1 text-base font-semibold tracking-tight"
            >
              {block.text}
            </h5>
          ) : (
            <p key={index} className="max-w-[90ch] text-[15px] leading-7">
              {block.text}
            </p>
          );
        }

        const headerRows = block.rows.slice(0, block.headerRowCount);
        const bodyRows = block.rows.slice(block.headerRowCount);
        return (
          <figure
            key={index}
            className="flex w-full min-w-0 max-w-full flex-col gap-2"
          >
            {block.title ? (
              <figcaption className="font-medium">{block.title}</figcaption>
            ) : null}
            <div className="w-full min-w-0 max-w-full overflow-x-auto rounded-lg border bg-background">
              <table
                className="w-max min-w-full border-collapse"
                aria-label={block.title || `Disclosure table ${index + 1}`}
              >
                {headerRows.length > 0 ? (
                  <thead>
                    <DisclosureRows rows={headerRows} header />
                  </thead>
                ) : null}
                <tbody>
                  <DisclosureRows rows={bodyRows} />
                </tbody>
              </table>
            </div>
          </figure>
        );
      })}
    </div>
  );
}
