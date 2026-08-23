import { redirect } from "react-router";

export function loader() {
  return redirect("/admin/se/people");
}

export default function AdminIndex() {
  return null;
}
