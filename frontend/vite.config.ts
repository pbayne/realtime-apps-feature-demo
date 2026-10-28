import { defineConfig } from "vite";
import tailwindcss from "@tailwindcss/vite";

// Build output lands in the FastAPI app's static dir (served at "/").
// Dev server proxies the backend API + WebSockets to the FastAPI app.
const backend = process.env.VITE_BACKEND ?? "http://localhost:8000";

export default defineConfig({
  plugins: [tailwindcss()],
  esbuild: { jsx: "automatic" },
  build: {
    outDir: "../src/app/static",
    emptyOutDir: true,
  },
  server: {
    port: 5173,
    proxy: {
      "/api": backend,
      "/ws": { target: backend.replace(/^http/, "ws"), ws: true },
    },
  },
});
