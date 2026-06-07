import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

export default defineConfig({
  plugins: [react()],
  server: {
    host: "0.0.0.0",
    port: 8787
  },
  build: {
    outDir: "dist/client",
    sourcemap: true
  }
});
