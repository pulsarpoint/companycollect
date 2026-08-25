import { redirect } from "react-router";

// The companies list used to live at /admin/se/company-info. It is now the Info
// tab of /admin/se/companies, so this thin route keeps old bookmarks and links
// working: a GET here 302s to the section's Info index. Resource route, loader
// only, no component.

export function loader() {
  throw redirect("/admin/se/companies");
}
