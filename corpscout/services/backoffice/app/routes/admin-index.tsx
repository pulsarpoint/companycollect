import { redirect } from "react-router";

export function loader() {
  return redirect("/admin/se/companies");
}

export default function AdminIndex() {
  return null;
}
