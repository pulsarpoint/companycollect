import { DomUtils, parseDocument } from "htmlparser2";
import {
  hasChildren,
  isTag,
  isText,
  type ChildNode,
  type Element,
} from "domhandler";

export interface EsefDisclosureTextBlock {
  type: "heading" | "paragraph";
  text: string;
}

export interface EsefDisclosureTableCell {
  text: string;
  colSpan: number;
  rowSpan: number;
}

export interface EsefDisclosureTableBlock {
  type: "table";
  title: string;
  headerRowCount: number;
  rows: EsefDisclosureTableCell[][];
}

export type EsefDisclosureBlock =
  EsefDisclosureTextBlock | EsefDisclosureTableBlock;

export interface EsefDisclosureDocument {
  blocks: EsefDisclosureBlock[];
  plainText: string;
}

const BLOCK_ELEMENTS = new Set([
  "article",
  "blockquote",
  "div",
  "h1",
  "h2",
  "h3",
  "h4",
  "h5",
  "h6",
  "li",
  "p",
  "section",
]);
const HEADING_ELEMENTS = new Set(["h1", "h2", "h3", "h4", "h5", "h6"]);
const IGNORED_ELEMENTS = new Set([
  "canvas",
  "iframe",
  "noscript",
  "script",
  "style",
  "svg",
]);
const TEXT_BOUNDARY_ELEMENTS = new Set([
  ...BLOCK_ELEMENTS,
  "br",
  "table",
  "tbody",
  "td",
  "tfoot",
  "th",
  "thead",
  "tr",
  "ol",
  "ul",
]);

function normalizedText(value: string): string {
  return value.replace(/\s+/gu, " ").trim();
}

function inlineText(nodes: ChildNode[]): string {
  let value = "";
  for (const node of nodes) {
    if (isText(node)) {
      value += node.data;
      continue;
    }
    if (!isTag(node) || IGNORED_ELEMENTS.has(node.name)) continue;
    if (node.name === "br") value += "\n";
    else value += inlineText(node.children);
  }
  return value;
}

function semanticText(nodes: ChildNode[]): string {
  let value = "";
  for (const node of nodes) {
    if (isText(node)) {
      value += node.data;
      continue;
    }
    if (!isTag(node) || IGNORED_ELEMENTS.has(node.name)) continue;
    const boundary = TEXT_BOUNDARY_ELEMENTS.has(node.name);
    if (boundary) value += " ";
    value += semanticText(node.children);
    if (boundary) value += " ";
  }
  return value;
}

function looksLikeHeading(text: string): boolean {
  if (text.length > 110) return false;
  const letters = text.match(/\p{L}/gu) ?? [];
  if (letters.length < 4) return false;
  const uppercaseLetters = letters.filter(
    (letter) => letter === letter.toLocaleUpperCase(),
  );
  return uppercaseLetters.length / letters.length >= 0.85;
}

function appendTextBlock(
  blocks: EsefDisclosureBlock[],
  value: string,
  heading = false,
): void {
  const text = normalizedText(value);
  if (!text) return;
  const type = heading || looksLikeHeading(text) ? "heading" : "paragraph";
  const previous = blocks.at(-1);
  if (
    type === "paragraph" &&
    previous?.type === "paragraph" &&
    previous.text.length + text.length < 2_400
  ) {
    previous.text = `${previous.text} ${text}`;
    return;
  }
  blocks.push({ type, text });
}

function tableRows(table: Element): Element[] {
  return DomUtils.findAll(
    (element) =>
      element.name === "tr" && nearestAncestorTable(element) === table,
    table.children,
  );
}

function nearestAncestorTable(node: ChildNode): Element | null {
  let ancestor = node.parent;
  while (ancestor) {
    if (isTag(ancestor) && ancestor.name === "table") return ancestor;
    ancestor = ancestor.parent;
  }
  return null;
}

function directCells(row: Element): Element[] {
  return row.children.filter(
    (node): node is Element =>
      isTag(node) && (node.name === "td" || node.name === "th"),
  );
}

function positiveSpan(value: string | undefined): number {
  const parsed = Number.parseInt(value ?? "1", 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : 1;
}

function numericCell(text: string): boolean {
  const compact = text.replace(/\s+/g, "");
  return Boolean(compact) && /^[+−–—-]?[\d.,%()/]+$/u.test(compact);
}

function headerRowCount(rows: EsefDisclosureTableCell[][]): number {
  const columnCount = Math.max(
    1,
    ...rows.map((row) => row.reduce((sum, cell) => sum + cell.colSpan, 0)),
  );
  const firstDataRow = rows.findIndex((row) => {
    const populated = row.filter((cell) => cell.text);
    if (populated.length < Math.max(2, Math.ceil(columnCount / 2)))
      return false;
    return (
      populated.filter((cell) => numericCell(cell.text)).length /
        populated.length >=
      0.5
    );
  });
  return firstDataRow > 0 ? firstDataRow : 0;
}

function parseTable(table: Element): EsefDisclosureTableBlock | null {
  const rows = tableRows(table)
    .map((row) =>
      directCells(row).map((cell) => ({
        text: normalizedText(inlineText(cell.children)),
        colSpan: positiveSpan(cell.attribs.colspan),
        rowSpan: positiveSpan(cell.attribs.rowspan),
      })),
    )
    .filter((row) => row.some((cell) => cell.text));
  if (rows.length === 0) return null;

  const firstPopulatedCells = rows[0].filter((cell) => cell.text);
  const title =
    rows.length > 1 && firstPopulatedCells.length === 1
      ? firstPopulatedCells[0].text
      : "";
  const contentRows = title ? rows.slice(1) : rows;
  return {
    type: "table",
    title,
    headerRowCount: headerRowCount(contentRows),
    rows: contentRows,
  };
}

function collectBlocks(
  nodes: ChildNode[],
  blocks: EsefDisclosureBlock[],
): void {
  let inlineBuffer = "";
  const flushInline = () => {
    appendTextBlock(blocks, inlineBuffer);
    inlineBuffer = "";
  };

  for (const node of nodes) {
    if (isText(node)) {
      inlineBuffer += node.data;
      continue;
    }
    if (!isTag(node) || IGNORED_ELEMENTS.has(node.name)) continue;
    if (node.name === "table") {
      flushInline();
      const table = parseTable(node);
      if (table) blocks.push(table);
      continue;
    }
    if (node.name === "br") {
      inlineBuffer += "\n";
      continue;
    }
    if (BLOCK_ELEMENTS.has(node.name)) {
      flushInline();
      const nestedTable = DomUtils.findOne(
        (element) => element.name === "table",
        node.children,
        true,
      );
      if (nestedTable) collectBlocks(node.children, blocks);
      else {
        appendTextBlock(
          blocks,
          inlineText(node.children),
          HEADING_ELEMENTS.has(node.name),
        );
      }
      continue;
    }
    if (hasChildren(node)) inlineBuffer += inlineText(node.children);
  }
  flushInline();
}

function documentPlainText(blocks: EsefDisclosureBlock[]): string {
  return blocks
    .map((block) => {
      if (block.type !== "table") return block.text;
      const rows = block.rows
        .map((row) =>
          row
            .map((cell) => cell.text)
            .join("\t")
            .trim(),
        )
        .filter(Boolean)
        .join("\n");
      return [block.title, rows].filter(Boolean).join("\n");
    })
    .filter(Boolean)
    .join("\n\n");
}

export function parseEsefDisclosure(rawValue: string): EsefDisclosureDocument {
  if (!rawValue.trim()) return { blocks: [], plainText: "" };
  const document = parseDocument(rawValue, { decodeEntities: true });
  const blocks: EsefDisclosureBlock[] = [];
  collectBlocks(document.children, blocks);
  return { blocks, plainText: documentPlainText(blocks) };
}

function persistedCell(value: unknown): EsefDisclosureTableCell | null {
  if (!value || typeof value !== "object") return null;
  const cell = value as Record<string, unknown>;
  if (
    typeof cell.text !== "string" ||
    !Number.isInteger(cell.colSpan) ||
    !Number.isInteger(cell.rowSpan) ||
    Number(cell.colSpan) < 1 ||
    Number(cell.rowSpan) < 1 ||
    Number(cell.colSpan) > 1_000 ||
    Number(cell.rowSpan) > 1_000
  ) {
    return null;
  }
  return {
    text: cell.text,
    colSpan: Number(cell.colSpan),
    rowSpan: Number(cell.rowSpan),
  };
}

function persistedBlock(value: unknown): EsefDisclosureBlock | null {
  if (!value || typeof value !== "object") return null;
  const block = value as Record<string, unknown>;
  if (
    (block.type === "heading" || block.type === "paragraph") &&
    typeof block.text === "string"
  ) {
    return { type: block.type, text: block.text };
  }
  if (
    block.type !== "table" ||
    typeof block.title !== "string" ||
    !Number.isInteger(block.headerRowCount) ||
    !Array.isArray(block.rows)
  ) {
    return null;
  }
  const rows: EsefDisclosureTableCell[][] = [];
  for (const rowValue of block.rows) {
    if (!Array.isArray(rowValue)) return null;
    const row: EsefDisclosureTableCell[] = [];
    for (const cellValue of rowValue) {
      const cell = persistedCell(cellValue);
      if (!cell) return null;
      row.push(cell);
    }
    rows.push(row);
  }
  const headerRowCount = Number(block.headerRowCount);
  if (headerRowCount < 0 || headerRowCount > rows.length) return null;
  return {
    type: "table",
    title: block.title,
    headerRowCount,
    rows,
  };
}

/** Validate source-derived blocks at the storage/UI boundary. Invalid or
 * stale artifacts fall back to deterministic parsing of the raw fact value. */
export function parsePersistedEsefDisclosure(
  blocksJson: string,
  plainText: string,
): EsefDisclosureDocument | null {
  if (!blocksJson) return null;
  try {
    const values = JSON.parse(blocksJson) as unknown;
    if (!Array.isArray(values)) return null;
    const blocks: EsefDisclosureBlock[] = [];
    for (const value of values) {
      const block = persistedBlock(value);
      if (!block) return null;
      blocks.push(block);
    }
    return { blocks, plainText };
  } catch {
    return null;
  }
}

export function esefDisclosureText(rawValue: string): string {
  if (!rawValue.includes("<")) return normalizedText(rawValue);
  const document = parseDocument(rawValue, { decodeEntities: true });
  return normalizedText(semanticText(document.children));
}
