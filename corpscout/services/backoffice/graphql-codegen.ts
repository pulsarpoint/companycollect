import "dotenv/config";

import type { CodegenConfig } from "@graphql-codegen/cli";

const schemaUrl = (
  process.env.DAGSTER_GRAPHQL_SCHEMA_URL ??
  process.env.DAGSTER_GRAPHQL_URL ??
  ""
).trim();

if (schemaUrl === "") {
  throw new Error(
    "Dagster schema generation requires DAGSTER_GRAPHQL_SCHEMA_URL or DAGSTER_GRAPHQL_URL.",
  );
}

const config: CodegenConfig = {
  schema: schemaUrl,
  documents: ["app/lib/dagster.operations.ts"],
  generates: {
    "app/lib/dagster.generated.ts": {
      plugins: ["typescript-operations"],
      config: {
        enumsAsTypes: true,
        nonOptionalTypename: true,
        scalars: {
          ID: { input: "string", output: "string" },
          GenericScalar: { input: "unknown", output: "unknown" },
          RunConfigData: {
            input: "Record<string, unknown>",
            output: "Record<string, unknown>",
          },
        },
        useTypeImports: true,
      },
    },
  },
};

export default config;
