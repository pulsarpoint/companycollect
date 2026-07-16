import { CircleMarker, MapContainer, TileLayer } from "react-leaflet";
import "leaflet/dist/leaflet.css";

export default function MiniMapInner({ lat, lon }: { lat: number; lon: number }) {
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
      <CircleMarker center={[lat, lon]} radius={8} pathOptions={{ fillOpacity: 0.7 }} />
    </MapContainer>
  );
}
