import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      // 通配所有以斜杠开头的 API 路径
      "^/(environments|devices|models|twins|relationships|import-excel)": {
        target: "http://localhost:8000",
        changeOrigin: true,
      },
    },
  },
});

