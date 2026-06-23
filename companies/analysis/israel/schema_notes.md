# Israel schema notes

## Fields observed

- `מספר חברה` - company number; primary key.
- `שם חברה` - Hebrew legal name.
- `שם באנגלית` - English name when present.
- `סוג תאגיד` - corporation type.
- `סטטוס חברה` and `קוד סטטוס חברה` - lifecycle status.
- `תאריך התאגדות` - incorporation date, observed as `DD/MM/YYYY`.
- `חברה ממשלתית`, `מגבלות`, `מפרה` - flags.
- `שנה אחרונה של דוח שנתי (שהוגש)` - last annual report year.
- address fields for city, street, house number, postcode, country.

## Mapping

- `registration_number`: company number
- `legal_name`: Hebrew company name
- `company_type`: corporation type
- `lifecycle_status`: company status
- `incorporation_date`: parse `DD/MM/YYYY`
- `registered_address`: concatenate address fields
