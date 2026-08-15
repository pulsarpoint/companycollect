import { renderToStaticMarkup } from "react-dom/server";
import { MemoryRouter } from "react-router";
import { describe, expect, it } from "vitest";
import {
  PersonContactCard,
  PossiblePersonMatchesCard,
} from "~/routes/country-person";

describe("country person profile sections", () => {
  it("does not attribute company contacts to a person", () => {
    const html = renderToStaticMarkup(<PersonContactCard contacts={[]} />);

    expect(html).toContain("No person-specific contact information");
    expect(html).toContain(
      "Company email addresses and phone numbers are not attributed",
    );
  });

  it("renders explicit public person contacts with their source", () => {
    const html = renderToStaticMarkup(
      <PersonContactCard
        contacts={[
          {
            contact_kind: "email",
            contact_value: "niklas@example.test",
            source: "official_bio",
            observation_id: "observation-1",
          },
        ]}
      />,
    );

    expect(html).toContain('href="mailto:niklas@example.test"');
    expect(html).toContain("official_bio");
  });

  it("labels same-name profiles as unconfirmed suggestions", () => {
    const html = renderToStaticMarkup(
      <MemoryRouter>
        <PossiblePersonMatchesCard
          countryCode="se"
          suggestions={[
            {
              reason: "compatible_relationship_and_name",
              person: {
                country_iso2: "SE",
                person_id: "22222222-2222-4222-8222-222222222222",
                preferred_name: "Niklas Thorén",
                preferred_name_normalized: "niklas thorén",
                resolution_status: "provisional",
                resolution_method: "same_company_name",
                merged_into_person_id: null,
                first_observed_year: 2024,
                last_observed_year: 2025,
                observation_count: 2,
                company_count: 1,
                resolved_at: "2026-08-14 10:00:00.000",
              },
              connections: [
                {
                  company_id: "5560000001",
                  company_name: "Possible Connection AB",
                  role_kind: "board_member",
                  role_original: "Styrelseledamot",
                  relationship_kind: "governance",
                  first_year: 2024,
                  last_year: 2025,
                  observation_count: 2,
                },
              ],
            },
          ]}
        />
      </MemoryRouter>,
    );

    expect(html).toContain("Possible same-person profiles");
    expect(html).toContain("not confirmed connections");
    expect(html).toContain("Board member");
    expect(html).toContain("at");
    expect(html).toContain("2 source observations");
    expect(html).toContain('href="/company/se/5560000001"');
    expect(html).toContain(
      'href="/country/se/person/22222222-2222-4222-8222-222222222222"',
    );
  });
});
