import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  // MapLibre loads a module worker at runtime. Pre-bundling it causes Vite to
  // look for a generated worker file that does not exist in the dependency
  // cache, leaving the map panel blank.
  optimizeDeps: {
    exclude: ["maplibre-gl"],
  },
  server: {
    host: "0.0.0.0",
    port: 5173,
  },
});
