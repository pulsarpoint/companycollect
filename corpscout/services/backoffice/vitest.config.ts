import { configDefaults, defineConfig } from "vitest/config";

export default defineConfig({
  resolve: {
    tsconfigPaths: true,
  },
  test: {
    setupFiles: ["./tests/setup.ts"],
    include: ["tests/**/*.test.{ts,tsx}", "app/**/*.test.{ts,tsx}"],
    // The ClickHouse-hitting tests never run unfiltered: they are excluded
    // unless the `test:live` script asks for them by name. A CLI --exclude is
    // ADDITIVE in vitest, so naming the file on the command line cannot lift a
    // config exclude -- the opt-in has to be here.
    exclude: [
      ...configDefaults.exclude,
      ...(process.env.VITEST_LIVE ? [] : ["tests/**/*.live.test.ts"]),
    ],
  },
});
