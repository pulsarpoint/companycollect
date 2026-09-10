import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, RouterProvider } from "react-router";
import { describe, expect, it } from "vitest";
import {
  EMPTY_PERSON_INITIAL,
  SePersonEditForm,
  type SePersonEditInitial,
} from "~/components/admin/se-person-edit-sheet";
import { SPLIT_CAVEAT } from "~/components/admin/se-person-workspace";

const COMPANY = "5560000001";
const KEY = "b7".repeat(32);
const SLOT = "r20260910120000123";
const ROLE_OPTIONS = [
  { code: "board_member", label: "Board member", group: "governance" },
  { code: "chief_executive_officer", label: "Chief executive officer", group: "executive" },
];
const published: SePersonEditInitial = {
  firstName: "Anna",
  lastName: "Svensson",
  birthYear: "1975",
  wikidataId: "Q7",
  roles: [{ code: "board_member", fromYear: "2023", toYear: "2025" }],
  data: '{"title":"Chair"}',
  note: "",
};

function render(element: React.ReactElement): string {
  const router = createMemoryRouter([{ path: "/admin/se/company/:companyId/people", element }], {
    initialEntries: [`/admin/se/company/${COMPANY}/people`],
  });
  return renderToStaticMarkup(<RouterProvider router={router} />);
}

describe("SePersonEditForm", () => {
  it("renders the person fields, the roles editor and the catalog's options in add mode", () => {
    const html = render(
      <SePersonEditForm
        mode="add"
        initial={EMPTY_PERSON_INITIAL}
        roleOptions={ROLE_OPTIONS}
        slot={null}
        replacesKey={null}
        result={null}
        onCancel={() => {}}
      />,
    );
    for (const name of ["first_name", "last_name", "birth_year", "wikidata_id", "role_code", "role_from", "role_to", "data", "note"]) {
      expect(html).toContain(`name="${name}"`);
    }
    expect(html).toContain('value="save-draft"');
    // Ruling 3: the options are the catalog's, by label, never a hard-coded list.
    expect(html).toContain('value="board_member"');
    expect(html).toContain("Board member");
    expect(html).toContain("Chief executive officer");
    // The particle hint, so a reviewer does not type "von Essen" as a first name.
    expect(html).toContain("von Essen");
  });

  it("prefills a Correct from the published person and carries the key it replaces", () => {
    const html = render(
      <SePersonEditForm
        mode="correct"
        initial={published}
        roleOptions={ROLE_OPTIONS}
        slot={null}
        replacesKey={KEY}
        result={null}
        onCancel={() => {}}
      />,
    );
    expect(html).toContain('value="Anna"');
    expect(html).toContain('value="Svensson"');
    expect(html).toContain('value="1975"');
    expect(html).toContain('value="Q7"');
    expect(html).toContain('value="2023"');
    expect(html).toContain(`name="replaces_key" value="${KEY}"`);
    expect(html).toContain("Correct person");
  });

  it("carries the draft's slot in edit-draft mode and shows this form's own refusal", () => {
    const html = render(
      <SePersonEditForm
        mode="edit-draft"
        initial={published}
        roleOptions={ROLE_OPTIONS}
        slot={SLOT}
        replacesKey={null}
        result={{ ok: false, intent: "save-draft", error: "initials only" }}
        onCancel={() => {}}
      />,
    );
    expect(html).toContain(`name="slot" value="${SLOT}"`);
    expect(html).toContain('role="alert"');
    expect(html).toContain("initials only");
  });

  it("warns, in the Split dialog's own words, that Bolagsverket re-slots every filing", () => {
    // Ruling 9 / spec 7's known limit for this slice. The dialog itself renders
    // nothing server-side (an open Dialog is client state), so the sentence is
    // exported as a string and pinned here -- the same trick `se-address-workspace.tsx`
    // uses for `removeDescription`.
    expect(SPLIT_CAVEAT).toContain("Bolagsverket mints a new slot per annual report");
    expect(SPLIT_CAVEAT).toContain("may need writing again after next year's report");
  });

  it("keeps another action's refusal out of the sheet", () => {
    const html = render(
      <SePersonEditForm
        mode="add"
        initial={EMPTY_PERSON_INITIAL}
        roleOptions={ROLE_OPTIONS}
        slot={null}
        replacesKey={null}
        result={{ ok: false, intent: "remove", error: "Already hidden." }}
        onCancel={() => {}}
      />,
    );
    expect(html).not.toContain("Already hidden.");
  });
});
