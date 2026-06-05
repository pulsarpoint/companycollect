import { Navigate, useParams, useSearchParams } from "react-router";

export default function SourceRawInputsRedirect() {
  const { name } = useParams<{ name: string }>();
  const [searchParams] = useSearchParams();
  const query = searchParams.toString();

  return (
    <Navigate
      to={`/sources/${name}/raw_input${query ? `?${query}` : ""}`}
      replace
    />
  );
}
