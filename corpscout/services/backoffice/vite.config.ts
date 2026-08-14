import { reactRouter } from "@react-router/dev/vite";
import tailwindcss from "@tailwindcss/vite";
import { defineConfig } from "vite";

export default defineConfig({
  server: { port: 5183 },
  plugins: [tailwindcss(), reactRouter()],
  optimizeDeps: {
    exclude: ["react-leaflet", "@react-leaflet/core"],
    include: ["leaflet"],
  },
  resolve: {
    dedupe: ["react", "react-dom"],
    tsconfigPaths: true,
  },
});
