const BASE = "/api/v1/db";

export interface PgRestPage<T> {
  data: T[];
  total: number;
}

export interface PgRestParams {
  select?: string;
  limit?: number;
  offset?: number;
  order?: string;
  [key: string]: string | number | string[] | undefined;
}

export async function pgrest<T>(table: string, params: PgRestParams = {}): Promise<PgRestPage<T>> {
  const qs = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) {
    if (v === undefined || v === "") continue;
    if (Array.isArray(v)) {
      for (const item of v) qs.append(k, item);
    } else {
      qs.set(k, String(v));
    }
  }
  const url = `${BASE}/${table}${qs.toString() ? `?${qs.toString()}` : ""}`;
  const res = await fetch(url, { headers: { Prefer: "count=exact" } });
  if (!res.ok) throw new Error(`PostgREST ${res.status}: ${await res.text()}`);

  const contentRange = res.headers.get("Content-Range") ?? "";
  const total = contentRange.includes("/") ? parseInt(contentRange.split("/")[1], 10) : 0;
  const data = (await res.json()) as T[];
  return { data, total };
}
