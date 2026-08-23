import assert from "node:assert/strict";
import test from "node:test";
import {cacheKey, createBrowserCache, planCacheWrite} from "./src/components/browser-cache.js";

test("cache keys include the schema and immutable content hash", () => {
  assert.equal(cacheKey(3, {sha256: "abc"}), "3:abc");
});

test("LRU planning invalidates stale schemas and stays within its byte budget", () => {
  const records = [
    {key: "old-schema", cacheSchemaVersion: 1, bytes: 40, lastAccess: 50},
    {key: "least-recent", cacheSchemaVersion: 2, bytes: 50, lastAccess: 10},
    {key: "most-recent", cacheSchemaVersion: 2, bytes: 30, lastAccess: 20},
  ];
  assert.deepEqual(planCacheWrite(records, {key: "incoming", bytes: 40}, 80, 2), {
    cacheable: true,
    deleteKeys: ["least-recent", "old-schema"],
  });
  assert.deepEqual(planCacheWrite(records, {key: "huge", bytes: 81}, 80, 2), {
    cacheable: false,
    deleteKeys: [],
  });
});

test("an unavailable or throwing IndexedDB never breaks reads or writes", async () => {
  const missing = createBrowserCache({indexedDBImpl: undefined});
  const entry = {sha256: "abc", bytes: 3};
  assert.equal(await missing.get(entry, 1), undefined);
  assert.equal(await missing.put(entry, 1, new ArrayBuffer(3)), false);
  assert.deepEqual(await missing.stats(), {entries: 0, bytes: 0, maxBytes: 96 * 1024 * 1024});

  const throwing = createBrowserCache({indexedDBImpl: {open() { throw new Error("private mode"); }}});
  assert.equal(await throwing.put(entry, 1, new ArrayBuffer(3)), false);
});
