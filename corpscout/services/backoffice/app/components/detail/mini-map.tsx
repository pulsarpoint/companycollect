import { Suspense, lazy, useEffect, useState } from "react";

const MiniMapInner = lazy(() => import("./mini-map-inner"));

export function MiniMap({
  lat,
  lon,
  approximate = false,
}: {
  lat: number;
  lon: number;
  approximate?: boolean;
}) {
  const [mounted, setMounted] = useState(false);
  useEffect(() => setMounted(true), []);
  if (!mounted) return <div className="bg-muted h-48 w-full rounded-md" />;
  return (
    <Suspense fallback={<div className="bg-muted h-48 w-full rounded-md" />}>
      <MiniMapInner lat={lat} lon={lon} approximate={approximate} />
    </Suspense>
  );
}
