import { CircleMarker, MapContainer, TileLayer } from "react-leaflet";
import "leaflet/dist/leaflet.css";

export default function MiniMapInner({
  lat,
  lon,
  approximate,
}: {
  lat: number;
  lon: number;
  approximate: boolean;
}) {
  return (
    <MapContainer
      center={[lat, lon]}
      zoom={14}
      scrollWheelZoom={false}
      className="h-48 w-full rounded-md"
    >
      <TileLayer
        url="https://tile.openstreetmap.org/{z}/{x}/{y}.png"
        attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
      />
      <CircleMarker
        center={[lat, lon]}
        radius={approximate ? 12 : 8}
        pathOptions={
          approximate
            ? {
                color: "#b45309",
                dashArray: "4 3",
                fillColor: "#f59e0b",
                fillOpacity: 0.25,
              }
            : { fillOpacity: 0.7 }
        }
      />
    </MapContainer>
  );
}
