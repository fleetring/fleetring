// Runs @libsql/client unchanged against the FleetRing router (FR-066, ADR-0037): `execute`, a
// batch that rolls back on error, a multi-statement script, interactive transactions that commit
// and roll back, arguments and values, and a refused token. Exits non-zero at the first failure.
//
//   ROUTER_URL=http://127.0.0.1:18080/db/app/ FLEETRING_TOKEN=… node conformance.mjs
//
// It also pins what ADR-0037 leaves to clients: that the client keeps the URL's path when it ends
// with a slash, accepts results without rows_read, rows_written, and query_duration_ms, reads the
// router's unpadded blobs, and reports a refusal's JSON error body.

import assert from "node:assert/strict";
import { createClient } from "@libsql/client";

const url = process.env.ROUTER_URL;
const authToken = process.env.FLEETRING_TOKEN;
assert.ok(url, "set ROUTER_URL to the router's URL for the database, with a trailing slash");
assert.ok(authToken, "set FLEETRING_TOKEN to a token with write on the database");

const client = createClient({ url, authToken });

async function scenario(name, run) {
  try {
    await run();
    console.log(`ok   ${name}`);
  } catch (error) {
    console.error(`FAIL ${name}`);
    console.error(error);
    process.exit(1);
  }
}

async function values() {
  const result = await client.execute("SELECT v FROM t ORDER BY id");
  return result.rows.map((row) => row.v);
}

await scenario("execute returns rows and the last insert row ID", async () => {
  await client.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");
  const inserted = await client.execute({
    sql: "INSERT INTO t (v) VALUES (?) RETURNING id",
    args: ["first"],
  });
  assert.equal(inserted.rowsAffected, 1);
  assert.equal(inserted.lastInsertRowid, 1n);
  assert.equal(inserted.rows[0].id, 1);
  assert.deepEqual(await values(), ["first"]);
});

await scenario("a batch rolls back when a step fails", async () => {
  await assert.rejects(
    client.batch(
      [
        { sql: "INSERT INTO t (v) VALUES (?)", args: ["second"] },
        "INSERT INTO missing VALUES (1)",
      ],
      "write",
    ),
  );
  assert.deepEqual(await values(), ["first"]);
});

await scenario("a multi-statement script runs", async () => {
  await client.executeMultiple(
    "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT);" +
      "INSERT INTO notes (body) VALUES ('a');" +
      "INSERT INTO notes (body) VALUES ('b');",
  );
  const counted = await client.execute("SELECT count(*) AS n FROM notes");
  assert.equal(counted.rows[0].n, 2);
});

await scenario("an interactive transaction commits", async () => {
  const transaction = await client.transaction("write");
  await transaction.execute({ sql: "INSERT INTO t (v) VALUES (?)", args: ["committed"] });
  const inside = await transaction.execute("SELECT count(*) AS n FROM t");
  assert.equal(inside.rows[0].n, 2);
  await transaction.commit();
  assert.deepEqual(await values(), ["first", "committed"]);
});

await scenario("an interactive transaction rolls back", async () => {
  const transaction = await client.transaction("write");
  await transaction.execute("INSERT INTO t (v) VALUES ('rolled back')");
  await transaction.rollback();
  assert.deepEqual(await values(), ["first", "committed"]);
});

await scenario("named and positional arguments, blobs, and floats round-trip", async () => {
  await client.execute("CREATE TABLE kinds (i INTEGER, r REAL, s TEXT, b BLOB, n)");
  await client.execute({
    sql: "INSERT INTO kinds VALUES (:i, :r, :s, :b, :n)",
    args: {
      i: -9007199254740993n,
      r: 1.5,
      s: "héllo",
      b: new Uint8Array([0, 1, 2, 255]),
      n: null,
    },
  });
  const client64 = createClient({ url, authToken, intMode: "bigint" });
  const read = await client64.execute({
    sql: "SELECT i, r, s, b, n FROM kinds WHERE s = ?",
    args: ["héllo"],
  });
  const row = read.rows[0];
  assert.equal(row.i, -9007199254740993n);
  assert.equal(row.r, 1.5);
  assert.equal(row.s, "héllo");
  assert.deepEqual([...new Uint8Array(row.b)], [0, 1, 2, 255]);
  assert.equal(row.n, null);
  client64.close();
});

await scenario("a refused token surfaces the router's error", async () => {
  const stranger = createClient({ url, authToken: "not-a-token" });
  await assert.rejects(stranger.execute("SELECT 1"), (error) => {
    console.log(`     refused with: ${error.message}`);
    return /token|UNAUTHENTICATED|401/i.test(String(error.message));
  });
  stranger.close();
});

client.close();
console.log("all scenarios passed");
