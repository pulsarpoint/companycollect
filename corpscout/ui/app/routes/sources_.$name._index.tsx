import { Navigate, useParams } from "react-router";
import { defaultSourceDetailPath } from "~/components/app/source-detail/sourceDetailUtils";

export default function SourceDetailIndex() {
  const { name } = useParams<{ name: string }>();
  if (!name) return <Navigate to="/sources" replace />;
  return <Navigate to={defaultSourceDetailPath(name)} replace />;
}
