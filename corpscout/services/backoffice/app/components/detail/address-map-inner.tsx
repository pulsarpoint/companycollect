import type { LatLngTuple } from "leaflet";
import {
  CircleMarker,
  MapContainer,
  type MapContainerProps,
  TileLayer,
  Tooltip,
} from "react-leaflet";
import "leaflet/dist/leaflet.css";
import type { AddressMapPoint } from "./address-map";

/**
 * The multi-point sibling of `mini-map-inner`: every geocoded address of one
 * company on a single map. Imports react-leaflet at module scope, so it is
 * only ever reached through `address-map.tsx`'s lazy import -- never
 * server-side.
 */

/** A geocode nobody should read as a doorstep: the loose amber disc of
 * `mini-map-inner`, kept identical so the two maps mean the same thing. */
const APPROXIMATE_STYLE = {
  color: "#b45309",
  dashArray: "4 3",
  fillColor: "#f59e0b",
  fillOpacity: 0.25,
} as const;

const EXACT_STYLE = { fillOpacity: 0.7 } as const;

export default function AddressMapInner({
  points,
  selectedKey,
  onSelect,
}: {
  points: readonly AddressMapPoint[];
  selectedKey: string | null;
  onSelect: (key: string) => void;
}) {
  // One address has no bounds worth fitting -- centre on it, and pull back a
  // little when the geocode is only a postcode or a city.
  const single = points.length === 1 ? points[0] : undefined;
  const fit: MapContainerProps = single
    ? { center: [single.lat, single.lon], zoom: single.approximate ? 12 : 14 }
    : {
        bounds: points.map((point): LatLngTuple => [point.lat, point.lon]),
        boundsOptions: { padding: [20, 20] },
      };
  return (
    <MapContainer {...fit} scrollWheelZoom={false} className="h-56 w-full rounded-md">
      <TileLayer
        url="https://tile.openstreetmap.org/{z}/{x}/{y}.png"
        attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
      />
      {points.map((point) => {
        const selected = point.key === selectedKey;
        return (
          <CircleMarker
            key={point.key}
            center={[point.lat, point.lon]}
            radius={point.approximate ? 12 : 8}
            pathOptions={{
              ...(point.approximate ? APPROXIMATE_STYLE : EXACT_STYLE),
              ...(selected ? { weight: 4 } : {}),
            }}
            eventHandlers={{ click: () => onSelect(point.key) }}
          >
            {/* Leaflet reads `permanent` when it binds the tooltip and never
                again, so the key remounts it when the selection moves --
                otherwise the first selected address would keep its open label
                after the reviewer clicked another. */}
            <Tooltip key={selected ? "permanent" : "hover"} permanent={selected}>
              {point.label}
            </Tooltip>
          </CircleMarker>
        );
      })}
    </MapContainer>
  );
}
