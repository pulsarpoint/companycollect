import { Badge } from "~/components/ui/badge";

// PRH trade-register status codes, per the YTJ v3 field catalog and the live
// snapshot: 1 = registered; 0 = current register entry is "not in register"
// (removed or never entered); 4 = ceased (full-register historical code).
// Unknown codes render as the raw code alone — never invent a label.
const FI_TRADE_REGISTER_STATUS_LABELS: Record<string, string> = {
  "0": "Not in trade register",
  "1": "Registered",
  "4": "Ceased",
};

export function fiTradeRegisterStatusText(value: unknown): string | null {
  if (value == null || value === "") return null;
  const code = String(value);
  const label = FI_TRADE_REGISTER_STATUS_LABELS[code];
  return label ? `${code} — ${label}` : code;
}

/** Finnish VAT numbers are derived, not registered: country code FI plus the
 * Business ID digits without the hyphen (vero.fi rule), valid only while the
 * company is VAT-registered. YTJ therefore publishes no VAT field and
 * fi_companies.vat_id is empty — derive it for display. */
export function fiVatId(record: Record<string, unknown>): string | null {
  const existing = record.vat_id;
  if (typeof existing === "string" && existing !== "") return existing;
  const vatRegistered =
    record.is_vat_registered === 1 ||
    record.is_vat_registered === "1" ||
    record.is_vat_registered === true;
  if (!vatRegistered) return null;
  const businessId = typeof record.business_id === "string" ? record.business_id : "";
  if (!/^\d{7}-\d$/.test(businessId)) return null;
  return `FI${businessId.replace("-", "")}`;
}

// The `status` source field is undocumented by PRH (constant "2" for active
// and ceased alike), so raw_status_code is intentionally left verbatim.
export function decorateFiRecord(
  record: Record<string, unknown>,
): Record<string, unknown> {
  const status = fiTradeRegisterStatusText(record.trade_register_status);
  const vatId = fiVatId(record);
  if (status == null && vatId == null) return record;
  const decorated = { ...record };
  if (status != null) decorated.trade_register_status = status;
  if (vatId != null) decorated.vat_id = vatId;
  return decorated;
}

const FI_REGISTRATION_FLAGS: [key: string, label: string][] = [
  ["is_vat_registered", "VAT"],
  ["is_employer_registered", "Employer"],
  ["is_prepayment_registered", "Prepayment"],
];

export function fiRegistrationFlags(
  record: Record<string, unknown>,
): { label: string; active: boolean }[] {
  return FI_REGISTRATION_FLAGS.map(([key, label]) => ({
    label,
    active: record[key] === 1 || record[key] === "1" || record[key] === true,
  }));
}

export function FiRegistryBadges({
  record,
}: {
  record: Record<string, unknown>;
}) {
  return (
    <div className="flex flex-wrap items-center gap-1.5">
      {fiRegistrationFlags(record).map(({ label, active }) => (
        <Badge
          key={label}
          variant={active ? "secondary" : "outline"}
          className={active ? "" : "text-muted-foreground"}
        >
          {label} {active ? "✓" : "✗"}
        </Badge>
      ))}
    </div>
  );
}
