import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// The engine's REST API and WebSocket (default http://localhost:8080).
const engine = process.env.SLUICE_API ?? "http://localhost:8080";
const api = ["/jobs", "/streams", "/sql", "/metrics", "/health"];

export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    port: 5173,
    proxy: Object.fromEntries(api.map((path) => [path, { target: engine, ws: path === "/jobs", changeOrigin: true }])),
  },
  build: {
    chunkSizeWarningLimit: 4000,
  },
});
