import { cloudflareTest } from "@cloudflare/vitest-pool-workers";
import { defineConfig } from "vitest/config";

export default defineConfig({
  plugins: [cloudflareTest({
    wrangler: { configPath: "./wrangler.toml" },
    miniflare: {
      bindings: {
        BUGLER_RETENTION_MAX_COUNT: "1",
        BUGLER_RETENTION_INTERVAL_MS: "60000",
      },
    },
  })],
  test: {
    include: ["tests/workers/**/*.test.ts"],
  },
});
