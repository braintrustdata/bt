import { Eval } from "braintrust";
import { version } from "bun";
import { Database } from "bun:sqlite";

const exactMatch = ({
  output,
  expected,
}: {
  output: boolean;
  expected?: boolean;
}) => ({
  name: "exact_match",
  score: output === expected ? 1 : 0,
});

const db = new Database(":memory:");
db.run("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)");
db.run("INSERT INTO test (name) VALUES (?)", ["Alice"]);

const query = db.query("SELECT name FROM test WHERE id = ?");
const result = query.get(1) as { name: string } | null;

Eval("test-cli-eval-bun", {
  experimentName: "Bun Imports Test",
  data: () => [
    { input: "bun_version", expected: true },
    { input: "sqlite_query", expected: true },
  ],
  task: async (input: string) => {
    if (input === "bun_version") {
      return typeof version === "string" && version.length > 0;
    }
    if (input === "sqlite_query") {
      return result?.name === "Alice";
    }
    return false;
  },
  scores: [exactMatch],
});

db.close();
