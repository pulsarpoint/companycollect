import "dotenv/config";
import { createClient } from "@clickhouse/client";

const username = process.env.CLICKHOUSE_WRITE_USER?.trim() ?? "";
const password = process.env.CLICKHOUSE_WRITE_PASSWORD ?? "";

if (!/^[a-zA-Z][a-zA-Z0-9_]{2,63}$/.test(username)) {
  throw new Error(
    "CLICKHOUSE_WRITE_USER must be a safe ClickHouse identifier.",
  );
}
if (password.length < 16) {
  throw new Error(
    "CLICKHOUSE_WRITE_PASSWORD must contain at least 16 characters.",
  );
}

const client = createClient({
  url: process.env.CLICKHOUSE_URL ?? "http://localhost:8123",
  username: process.env.CLICKHOUSE_USER ?? "default",
  password: process.env.CLICKHOUSE_PASSWORD ?? "",
  database: process.env.CLICKHOUSE_DATABASE ?? "corpscout",
});

try {
  await client.command({
    query: `CREATE USER IF NOT EXISTS ${username} IDENTIFIED WITH sha256_password BY {writerPassword:String}`,
    query_params: { writerPassword: password },
  });
  await client.command({
    query: `ALTER USER ${username} IDENTIFIED WITH sha256_password BY {writerPassword:String}`,
    query_params: { writerPassword: password },
  });
  await client.command({
    query: `GRANT corpscout_person_correction_writer TO ${username}`,
  });
  await client.command({
    query: `SET DEFAULT ROLE corpscout_person_correction_writer TO ${username}`,
  });
  process.stdout.write(
    `Provisioned ClickHouse correction writer ${username}.\n`,
  );
} finally {
  await client.close();
}
