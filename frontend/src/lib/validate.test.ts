import assert from "node:assert/strict";
import { test } from "node:test";

import type { BuilderGraph, JobSettings, NodeData } from "../types/pipeline.ts";
import { defaultData, definitionToGraph, findCycle, parseLiteral, toJobDefinition, validate } from "./validate.ts";

const settings: JobSettings = {
  name: "spend",
  checkpointing: true,
  intervalMs: 1000,
  backend: "rocksdb",
  retained: 3,
  maxRestarts: 3,
  restartDelayMs: 200,
};

function graph(kinds: NodeData["kind"][], edges: [number, number][]): BuilderGraph {
  return {
    nodes: kinds.map((kind, i) => ({ id: `n${i}`, data: defaultData(kind) })),
    edges: edges.map(([a, b]) => ({ source: `n${a}`, target: `n${b}` })),
  };
}

const linear = () =>
  graph(["source", "filter", "key_by", "window", "sink"], [
    [0, 1],
    [1, 2],
    [2, 3],
    [3, 4],
  ]);

test("a linear pipeline validates and serializes in path order", () => {
  const g = linear();
  g.edges.reverse(); // edge order must not matter
  const result = validate(g);
  assert.deepEqual(result.issues, []);
  assert.deepEqual(result.chain, ["n0", "n1", "n2", "n3", "n4"]);

  const job = toJobDefinition(g, result.chain!, settings);
  assert.equal(job.name, "spend");
  assert.deepEqual(job.source, { type: "grpc", stream: "purchases", max_out_of_orderness_ms: 2000 });
  assert.deepEqual(
    job.operators?.map((o) => o.type),
    ["filter", "key_by", "window"],
  );
  assert.deepEqual(job.operators?.[0], { type: "filter", field: "amount", op: "gt", value: 0 });
  assert.deepEqual(job.checkpointing, { interval_ms: 1000, backend: "rocksdb", retained: 3 });
  assert.deepEqual(job.sink, { type: "buffer", capacity: 5000 });
});

test("cycles are detected", () => {
  const g = graph(["source", "filter", "map", "sink"], [
    [0, 1],
    [1, 2],
    [2, 1],
    [2, 3],
  ]);
  assert.deepEqual([...findCycle(g)].sort(), ["n1", "n2", "n3"]);
  const result = validate(g);
  assert.equal(result.chain, null);
  assert.ok(result.issues.some((i) => i.nodeId === "n1" && /cycle/.test(i.message)));
});

test("every operator must be on the Source → Sink path", () => {
  const g = graph(["source", "filter", "sink", "map"], [
    [0, 1],
    [1, 2],
  ]);
  const result = validate(g);
  assert.deepEqual(
    result.issues.map((i) => i.nodeId),
    ["n3"],
  );
  assert.match(result.issues[0].message, /not connected/);

  const dangling = validate(graph(["source", "filter", "sink"], [[0, 1]]));
  assert.ok(dangling.issues.some((i) => i.nodeId === "n1" && /does not lead to the Sink/.test(i.message)));
});

test("Key By must precede Window", () => {
  const g = graph(["source", "window", "key_by", "sink"], [
    [0, 1],
    [1, 2],
    [2, 3],
  ]);
  assert.ok(validate(g).issues.some((i) => i.nodeId === "n1" && /Key By before/.test(i.message)));
});

test("fan-out, fan-in, joins and missing config are reported", () => {
  const fan = validate(
    graph(["source", "filter", "map", "sink"], [
      [0, 1],
      [0, 2],
      [1, 3],
      [2, 3],
    ]),
  );
  assert.ok(fan.issues.some((i) => i.nodeId === "n0" && /fan out/.test(i.message)));
  assert.ok(fan.issues.some((i) => i.nodeId === "n3" && /several inputs/.test(i.message)));

  const join = validate(graph(["source", "join", "sink"], [
    [0, 1],
    [1, 2],
  ]));
  assert.ok(join.issues.some((i) => i.nodeId === "n1" && /Rust API/.test(i.message)));

  const g = linear();
  g.nodes[3].data = { ...(g.nodes[3].data as Extract<NodeData, { kind: "window" }>), windowKind: "sliding", slideMs: 120_000 };
  assert.ok(validate(g).issues.some((i) => /slide/.test(i.message)));

  assert.match(validate({ nodes: [], edges: [] }).issues[0].message, /Source/);
});

test("filter literals", () => {
  assert.equal(parseLiteral("10"), 10);
  assert.equal(parseLiteral("-2.5"), -2.5);
  assert.equal(parseLiteral("true"), true);
  assert.equal(parseLiteral("'bob'"), "bob");
  assert.equal(parseLiteral("bob"), "bob");
});

test("job definitions round-trip through the builder graph", () => {
  const g = linear();
  g.nodes[1].data = { kind: "filter", field: "user_id", op: "ne", value: "'bot'" };
  const original = toJobDefinition(g, validate(g).chain!, settings);
  const { graph, settings: restored } = definitionToGraph(original);
  const result = validate(graph);
  assert.deepEqual(result.issues, []);
  const again = toJobDefinition(graph, result.chain!, { ...settings, ...restored });
  assert.deepEqual(again, original);
});
