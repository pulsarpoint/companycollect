import { Suspense, lazy, useEffect, useState, type ReactNode } from "react";

/**
 * The client-only wrapper around `address-map-inner`, built like `mini-map`:
 * react-leaflet touches `window` at import time, so the map is lazily imported
 * and only rendered once the component has mounted in a browser. Server-side
 * -- and in the tests, which render with `renderToStaticMarkup` and no DOM --
 * what renders is the placeholder.
 */

/** One geocoded address on the map. `key` is the address key a click selects;
 * `approximate` draws the loose marker instead of the solid one. */
export interface AddressMapPoint {
  key: string;
  lat: number;
  lon: number;
  label: string;
  approximate: boolean;
}

const AddressMapInner = lazy(() => import("./address-map-inner"));

/** The map's footprint before (or instead of) the map, so the column does not
 * jump when leaflet arrives. */
function MapPlaceholder({ children }: { children?: ReactNode }) {
  return (
    <div className="bg-muted text-muted-foreground flex h-56 w-full items-center justify-center rounded-md text-xs">
      {children}
    </div>
  );
}

export function AddressMap({
  points,
  selectedKey,
  onSelect,
}: {
  points: readonly AddressMapPoint[];
  selectedKey: string | null;
  onSelect: (key: string) => void;
}) {
  const [mounted, setMounted] = useState(false);
  useEffect(() => setMounted(true), []);
  // An address without a usable geocode says so rather than showing an empty
  // map of Sweden -- and `bounds` of nothing is not a map leaflet can fit.
  if (points.length === 0) return <MapPlaceholder>No location</MapPlaceholder>;
  if (!mounted) return <MapPlaceholder />;
  return (
    <Suspense fallback={<MapPlaceholder />}>
      <AddressMapInner points={points} selectedKey={selectedKey} onSelect={onSelect} />
    </Suspense>
  );
}
