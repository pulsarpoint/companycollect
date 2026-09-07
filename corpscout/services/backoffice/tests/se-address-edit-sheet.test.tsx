import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, RouterProvider } from "react-router";
import { describe, expect, it } from "vitest";
import {
  EMPTY_ADDRESS_INITIAL,
  SeAddressEditForm,
  SeAddressEditSheet,
  type SeAddressEditInitial,
} from "~/components/admin/se-address-edit-sheet";
import { addressKindLabel, REVIEWER_KINDS } from "~/lib/se-address-fields";

const COMPANY = "0113004022";
/** A 64-hex address key, as `isAddressKey` requires. */
const KEY = "b7".repeat(32);
/** `r` + 17 digits, as the decision parser's slot pattern requires. */
const SLOT = "r20260905090000000";

const published: SeAddressEditInitial = {
  careOf: "c/o Anna Andersson",
  streetLine: "Storgatan 5 A",
  postalCode: "11122",
  city: "Stockholm",
  kind: "visiting",
  note: "",
};

function render(element: React.ReactElement): string {
  const router = createMemoryRouter([{ path: "/admin/se/company/:companyId/address", element }], {
    initialEntries: [`/admin/se/company/${COMPANY}/address`],
  });
  return renderToStaticMarkup(<RouterProvider router={router} />);
}

describe("SeAddressEditForm", () => {
  it("renders the seven fields with a read-only Swedish country in add mode", () => {
    const html = render(
      <SeAddressEditForm
        mode="add"
        initial={EMPTY_ADDRESS_INITIAL}
        slot={null}
        replacesKey={null}
        result={null}
        onCancel={() => {}}
      />,
    );
    for (const name of [
      "care_of",
      "street_line",
      "postal_code",
      "city",
      "country",
      "kind",
      "note",
    ]) {
      expect(html).toContain(`name="${name}"`);
    }
    expect(html).toContain('placeholder="Storgatan 5 or Box 123"');
    // Sweden only (the client-safe validation refuses anything else), so the
    // country is shown but not typeable.
    expect(html).toContain('value="SE"');
    // Base UI passes the prop through verbatim, so match either casing.
    expect(html).toMatch(/readonly=/i);
    expect(html).toContain('data-slot="textarea"');
    expect(html).toContain("Save draft");
    expect(html).toContain("Cancel");
  });

  it("offers exactly the reviewer kinds in the kind select", () => {
    const html = render(
      <SeAddressEditForm
        mode="add"
        initial={EMPTY_ADDRESS_INITIAL}
        slot={null}
        replacesKey={null}
        result={null}
        onCancel={() => {}}
      />,
    );
    for (const kind of REVIEWER_KINDS) {
      expect(html).toContain(`value="${kind}"`);
      expect(html).toContain(addressKindLabel(kind));
    }
    expect((html.match(/<option/g) ?? []).length).toBe(REVIEWER_KINDS.length);
    // `workplace` and `unknown` are source-only kinds: a reviewer never types them.
    expect(html).not.toContain('value="workplace"');
    expect(html).not.toContain('value="unknown"');
  });

  it("posts the hidden intent with an empty slot and replaces_key in add mode", () => {
    const html = render(
      <SeAddressEditForm
        mode="add"
        initial={EMPTY_ADDRESS_INITIAL}
        slot={null}
        replacesKey={null}
        result={null}
        onCancel={() => {}}
      />,
    );
    expect(html).toContain('name="intent"');
    expect(html).toContain('value="save-draft"');
    expect(html).toContain('name="slot"');
    expect(html).toContain('name="replaces_key"');
    expect(html).not.toContain(KEY);
    expect(html).not.toContain(SLOT);
  });

  it("prefills from the published address and carries replaces_key in correct mode", () => {
    const html = render(
      <SeAddressEditForm
        mode="correct"
        initial={published}
        slot={null}
        replacesKey={KEY}
        result={null}
        onCancel={() => {}}
      />,
    );
    expect(html).toContain('value="c/o Anna Andersson"');
    expect(html).toContain('value="Storgatan 5 A"');
    expect(html).toContain('value="11122"');
    expect(html).toContain('value="Stockholm"');
    expect(html).toContain('value="visiting" selected');
    expect(html).toContain(`value="${KEY}"`);
  });

  it("carries the slot it is editing in edit-draft mode", () => {
    const html = render(
      <SeAddressEditForm
        mode="edit-draft"
        initial={{ ...published, note: "typed from the annual report" }}
        slot={SLOT}
        replacesKey={null}
        result={null}
        onCancel={() => {}}
      />,
    );
    expect(html).toContain(`value="${SLOT}"`);
    expect(html).toContain("typed from the annual report");
  });

  it("shows a refusal as an alert", () => {
    const html = render(
      <SeAddressEditForm
        mode="add"
        initial={EMPTY_ADDRESS_INITIAL}
        slot={null}
        replacesKey={null}
        result={{ ok: false, error: "Postcode must be five digits." }}
        onCancel={() => {}}
      />,
    );
    expect(html).toContain('role="alert"');
    expect(html).toContain("Postcode must be five digits.");
  });
});

describe("SeAddressEditSheet", () => {
  it("renders nothing while closed", () => {
    const html = render(
      <SeAddressEditSheet
        open={false}
        onOpenChange={() => {}}
        mode="add"
        initial={EMPTY_ADDRESS_INITIAL}
        slot={null}
        replacesKey={null}
        result={null}
      />,
    );
    expect(html).toBe("");
  });
});
