export const DEFAULT_CACHE_MAX_BYTES = 96 * 1024 * 1024;
const DATABASE_NAME = "wiki-econ-browser-data";
const DATABASE_VERSION = 1;
const STORE_NAME = "partitions";

export function cacheKey(cacheSchemaVersion, entry) {
  return `${cacheSchemaVersion}:${entry.sha256}`;
}

export function planCacheWrite(records, incoming, maxBytes, cacheSchemaVersion) {
  if (incoming.bytes > maxBytes) return {cacheable: false, deleteKeys: []};
  const stale = records.filter(record => record.cacheSchemaVersion !== cacheSchemaVersion);
  const current = records
    .filter(record => record.cacheSchemaVersion === cacheSchemaVersion && record.key !== incoming.key)
    .sort((left, right) => left.lastAccess - right.lastAccess || left.key.localeCompare(right.key));
  let retainedBytes = current.reduce((total, record) => total + record.bytes, 0);
  const deleteKeys = stale.map(record => record.key);
  while (retainedBytes + incoming.bytes > maxBytes && current.length > 0) {
    const victim = current.shift();
    retainedBytes -= victim.bytes;
    deleteKeys.push(victim.key);
  }
  return {cacheable: true, deleteKeys: [...new Set(deleteKeys)].sort()};
}

function requestValue(request) {
  return new Promise((resolve, reject) => {
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error || new Error("IndexedDB request failed"));
  });
}

function transactionDone(transaction) {
  return new Promise((resolve, reject) => {
    transaction.oncomplete = () => resolve();
    transaction.onerror = () => reject(transaction.error || new Error("IndexedDB transaction failed"));
    transaction.onabort = () => reject(transaction.error || new Error("IndexedDB transaction aborted"));
  });
}

export function createBrowserCache({
  indexedDBImpl = globalThis.indexedDB,
  maxBytes = DEFAULT_CACHE_MAX_BYTES,
  now = () => Date.now(),
} = {}) {
  let databasePromise;

  function open() {
    if (databasePromise) return databasePromise;
    databasePromise = new Promise(resolve => {
      if (!indexedDBImpl) return resolve(null);
      let request;
      try {
        request = indexedDBImpl.open(DATABASE_NAME, DATABASE_VERSION);
      } catch {
        return resolve(null);
      }
      request.onupgradeneeded = () => {
        const database = request.result;
        if (database.objectStoreNames.contains(STORE_NAME)) database.deleteObjectStore(STORE_NAME);
        const store = database.createObjectStore(STORE_NAME, {keyPath: "key"});
        store.createIndex("lastAccess", "lastAccess");
      };
      request.onsuccess = () => resolve(request.result);
      request.onerror = () => resolve(null);
      request.onblocked = () => resolve(null);
    });
    return databasePromise;
  }

  async function get(entry, cacheSchemaVersion) {
    try {
      const database = await open();
      if (!database) return undefined;
      const key = cacheKey(cacheSchemaVersion, entry);
      const record = await requestValue(database.transaction(STORE_NAME, "readonly").objectStore(STORE_NAME).get(key));
      if (!record || record.sha256 !== entry.sha256 || record.bytes !== entry.bytes) return undefined;
      const transaction = database.transaction(STORE_NAME, "readwrite");
      transaction.objectStore(STORE_NAME).put({...record, lastAccess: now()});
      void transactionDone(transaction).catch(() => {});
      return record.payload;
    } catch {
      return undefined;
    }
  }

  async function put(entry, cacheSchemaVersion, payload) {
    try {
      const database = await open();
      if (!database || payload.byteLength !== entry.bytes) return false;
      const existing = await requestValue(database.transaction(STORE_NAME, "readonly").objectStore(STORE_NAME).getAll());
      const key = cacheKey(cacheSchemaVersion, entry);
      const plan = planCacheWrite(existing, {key, bytes: entry.bytes}, maxBytes, cacheSchemaVersion);
      if (!plan.cacheable) return false;
      const transaction = database.transaction(STORE_NAME, "readwrite");
      const store = transaction.objectStore(STORE_NAME);
      for (const staleKey of plan.deleteKeys) store.delete(staleKey);
      store.put({key, cacheSchemaVersion, sha256: entry.sha256, bytes: entry.bytes, lastAccess: now(), payload});
      await transactionDone(transaction);
      return true;
    } catch {
      return false;
    }
  }

  async function stats() {
    try {
      const database = await open();
      if (!database) return {entries: 0, bytes: 0, maxBytes};
      const records = await requestValue(database.transaction(STORE_NAME, "readonly").objectStore(STORE_NAME).getAll());
      return {entries: records.length, bytes: records.reduce((total, record) => total + record.bytes, 0), maxBytes};
    } catch {
      return {entries: 0, bytes: 0, maxBytes};
    }
  }

  return {get, put, stats, maxBytes};
}
