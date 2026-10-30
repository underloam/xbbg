import { fileURLToPath } from "node:url";
import { defineConfig } from "vitest/config";

export default defineConfig({
  resolve: {
    alias: {
      "@xbbg/core": fileURLToPath(new URL("../js-xbbg/src/index.ts", import.meta.url)),
    },
  },
  test: {
    environment: "node",
    globals: true,
    hookTimeout: 30000,
    include: ["test/**/*.test.ts"],
    pool: "forks",
    reporters: ["default"],
    testTimeout: 30000,
  },
});
