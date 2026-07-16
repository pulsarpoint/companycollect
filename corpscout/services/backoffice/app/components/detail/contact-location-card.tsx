import { useEffect, useRef } from "react";
import { useFetcher } from "react-router";
import { Globe, Mail, Phone, Printer, Smartphone } from "lucide-react";
import type { CountryConfig } from "~/lib/countries";
import type { AddressRow, ContactRow } from "~/lib/queries.server";
import { humanizeFieldKey } from "~/components/detail/fields";
import { Badge } from "~/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "~/components/ui/card";
import { MiniMap } from "~/components/detail/mini-map";

const CONTACT_ICONS: Record<string, typeof Mail> = {
  email: Mail,
  phone: Phone,
  mobile: Smartphone,
  fax: Printer,
  website: Globe,
};

function storedCoords(record: Record<string, unknown>): { lat: number; lon: number } | null {
  const lat = record.address_latitude;
  const lon = record.address_longitude;
  return typeof lat === "number" && typeof lon === "number" ? { lat, lon } : null;
}

export function ContactLocationCard({
  country,
  contacts,
  addresses,
  record,
}: {
  country: CountryConfig;
  contacts: ContactRow[];
  addresses: AddressRow[];
  record: Record<string, unknown>;
}) {
  const fetcher = useFetcher<{ coords: { lat: number; lon: number } | null }>();
  const requested = useRef(false);
  const stored = storedCoords(record);
  const realAddresses = addresses.filter((a) => a.full_address.trim() !== "");
  const candidateTarget =
    !stored && realAddresses.length > 0 ? realAddresses[0].full_address : null;
  // Over-long addresses are unresolvable: no fetch, no map — same as no address at all.
  const geocodeTarget = candidateTarget && candidateTarget.length <= 300 ? candidateTarget : null;

  useEffect(() => {
    if (geocodeTarget && !requested.current && fetcher.state === "idle" && fetcher.data === undefined) {
      requested.current = true;
      fetcher.load(
        `/${country.code}/geocode?address=${encodeURIComponent(geocodeTarget)}`,
      );
    }
  }, [geocodeTarget, fetcher, country.code]);

  if (contacts.length === 0 && realAddresses.length === 0) return null;
  const coords = stored ?? fetcher.data?.coords ?? null;

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">Contact &amp; location</CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        {contacts.length > 0 ? (
          <ul className="space-y-1.5">
            {contacts.map((c, i) => {
              const Icon = CONTACT_ICONS[c.contact_type];
              const isLink = c.contact_type === "website" || c.contact_value.startsWith("http");
              return (
                <li key={`${c.contact_type}-${c.contact_value}-${i}`} className="flex items-baseline gap-2 text-sm">
                  {Icon ? (
                    <Icon className="text-muted-foreground size-3.5 shrink-0 self-center" />
                  ) : (
                    <Badge variant="outline">{c.contact_type}</Badge>
                  )}
                  {isLink ? (
                    <a
                      href={c.contact_value.startsWith("http") ? c.contact_value : `https://${c.contact_value}`}
                      target="_blank"
                      rel="noreferrer"
                      className="break-all underline underline-offset-2"
                    >
                      {c.contact_value}
                    </a>
                  ) : (
                    <span className="break-all">{c.contact_value}</span>
                  )}
                </li>
              );
            })}
          </ul>
        ) : null}

        {realAddresses.map((a, i) => (
          <div key={`${a.address_type}-${i}`} className="text-sm">
            <p className="text-muted-foreground text-xs font-medium uppercase tracking-wide">
              {humanizeFieldKey(a.address_type)}
            </p>
            <p>{a.full_address}</p>
          </div>
        ))}

        {coords ? (
          <MiniMap lat={coords.lat} lon={coords.lon} />
        ) : geocodeTarget && fetcher.state !== "idle" ? (
          <div className="bg-muted text-muted-foreground flex h-48 w-full items-center justify-center rounded-md text-xs">
            Locating…
          </div>
        ) : null}
      </CardContent>
    </Card>
  );
}
