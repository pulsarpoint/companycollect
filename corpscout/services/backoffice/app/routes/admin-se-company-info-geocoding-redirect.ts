import { redirect } from "react-router";

// Geocoding used to live at /admin/se/company-info/geocoding. It is now the
// Geocoding tab of /admin/se/companies, so this thin route keeps old bookmarks
// and links working: a GET here 302s to the new tab. Resource route, loader
// only, no component.

export function loader() {
  throw redirect("/admin/se/companies/geocoding");
}
