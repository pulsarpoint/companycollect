import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import { EsefDisclosureReader } from "~/components/detail/esef-disclosure-reader";
import {
  esefDisclosureText,
  parseEsefDisclosure,
  parsePersistedEsefDisclosure,
} from "~/lib/esef-disclosures";

const disclosureXhtml = `
  N<span class="letter-spacing"></span>OT<span> </span>13<span> </span>FÖRVALTNINGSFASTIGHETER
  <table>
    <tr><td>Property portfolio</td><td></td><td></td></tr>
    <tr><td>Market</td><td>Area, sqm</td><td>Fair value</td></tr>
    <tr><td>Sweden</td><td>935<span> </span>000</td><td>15<span></span>101</td></tr>
    <tr><td>Finland</td><td>1<span> </span>480<span> </span>000</td><td>19<span> </span>606</td></tr>
  </table>
  <p>Values are reported in SEK &amp; reviewed annually.</p>
  <script>do not render this</script>
`;

describe("ESEF disclosure parsing", () => {
  it("uses DOM text semantics instead of inserting spaces at every tag", () => {
    expect(esefDisclosureText(disclosureXhtml)).toContain(
      "NOT 13 FÖRVALTNINGSFASTIGHETER",
    );
    expect(esefDisclosureText(disclosureXhtml)).not.toContain("N OT");
    expect(esefDisclosureText(disclosureXhtml)).toContain("15101");
    expect(esefDisclosureText(disclosureXhtml)).not.toContain("15 101");
    expect(esefDisclosureText(disclosureXhtml)).not.toContain(
      "do not render this",
    );
  });

  it("preserves headings, table structure, and paragraphs", () => {
    const disclosure = parseEsefDisclosure(disclosureXhtml);
    const table = disclosure.blocks.find((block) => block.type === "table");

    expect(disclosure.blocks[0]).toEqual({
      type: "heading",
      text: "NOT 13 FÖRVALTNINGSFASTIGHETER",
    });
    expect(table).toMatchObject({
      type: "table",
      title: "Property portfolio",
      headerRowCount: 1,
    });
    expect(table?.type === "table" ? table.rows[1][0].text : "").toBe("Sweden");
    expect(disclosure.plainText).toContain(
      "Values are reported in SEK & reviewed annually.",
    );
  });

  it("renders only parsed text and semantic table cells", () => {
    const html = renderToStaticMarkup(
      <EsefDisclosureReader
        disclosure={parseEsefDisclosure(disclosureXhtml)}
      />,
    );

    expect(html).toContain("<table");
    expect(html).toContain("<figcaption");
    expect(html).toContain("Property portfolio");
    expect(html).toContain("<thead>");
    expect(html).toContain("Sweden");
    expect(html).not.toContain("<script>");
    expect(html).not.toContain("do not render this");
  });

  it("validates persisted disclosure blocks before rendering them", () => {
    const parsed = parsePersistedEsefDisclosure(
      JSON.stringify([
        { type: "heading", text: "BUSINESS" },
        {
          type: "table",
          title: "Markets",
          headerRowCount: 1,
          rows: [
            [
              { text: "Country", colSpan: 1, rowSpan: 1 },
              { text: "Sales", colSpan: 1, rowSpan: 1 },
            ],
          ],
        },
      ]),
      "BUSINESS\n\nMarkets\nCountry\tSales",
    );

    expect(parsed?.blocks).toHaveLength(2);
    expect(parsed?.plainText).toContain("Country\tSales");
    expect(
      parsePersistedEsefDisclosure(
        JSON.stringify([
          {
            type: "table",
            title: "Broken",
            headerRowCount: 1,
            rows: [[{ text: "Cell", colSpan: 0, rowSpan: 1 }]],
          },
        ]),
        "Broken",
      ),
    ).toBeNull();
  });
});
