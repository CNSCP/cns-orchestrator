// registry.js - resolve Connection Profiles from the CP Registry
// Copyright 2025 Padi, Inc. All Rights Reserved.
//
// The 2026 resolution contract (CNS/CP specification §7.4; CNSCP/registry):
//
//   GET {origin}/{name}      the selection surface: is the name registered,
//                            which versions exist, and each one's status. It
//                            is no-cache by design, so it is held with its
//                            ETag and revalidated (If-None-Match, 304).
//   GET {origin}/{name}:{n}  one version's contract. A published version
//                            never changes, so once fetched it is kept.
//
// Both with Accept: application/cp+json; profile=2026.
//
// What is held, and for how long:
//
//   - contracts: for good. They are immutable.
//   - surfaces: until revalidate() refreshes them. Lifecycle (Deprecation) and
//     newly published versions reach the resolver that way.
//   - "not registered", "nothing published", "no such version": until the
//     next revalidate(), then asked again. A name can be registered, and a
//     version published, at any time.
//   - transient failures (unreachable, timeout, 5xx, unreadable reply): never
//     held as an answer. The same name is not asked again for RETRY_GAP ms,
//     so a Registry outage does not turn every build into a request storm.
//
// A version is chosen by its number, as the declaration states it. Nothing is
// taken from the contract's Header, which older versions may lack.

'use strict';

const http = require('http');
const https = require('https');

const ACCEPT = 'application/cp+json; profile=2026';

// Why a Profile did not resolve
const UNREGISTERED = 'not registered';
const UNPUBLISHED = 'nothing published';
const NO_VERSION = 'no such version';
const DEPRECATED = 'deprecated';
const UNAVAILABLE = 'registry unavailable';

class ResolveError extends Error {
  constructor(kind, name, version, detail) {
    super(name + (version !== undefined ? (':' + version) : '') + ' ' + kind +
      (detail ? (' (' + detail + ')') : ''));
    this.kind = kind;
  }
}

// Plain GET with a timeout; resolves { status, headers, body }
function httpGet(url, headers, timeout) {
  return new Promise((resolve, reject) => {
    const u = new URL(url);
    const lib = (u.protocol === 'https:') ? https : http;

    const req = lib.get(u, { headers: headers, agent: (u.protocol === 'https:') ? httpsAgent : httpAgent }, (res) => {
      var body = '';

      res.setEncoding('utf8');
      res.on('data', (chunk) => { body += chunk; });
      res.on('end', () => resolve({ status: res.statusCode, headers: res.headers, body: body }));
      res.on('error', reject);
    });

    req.setTimeout(timeout, () => req.destroy(new Error('timed out after ' + timeout + 'ms')));
    req.on('error', reject);
  });
}

const httpAgent = new http.Agent({ keepAlive: true });
const httpsAgent = new https.Agent({ keepAlive: true });

// The Registry's origin, from a setting: no trailing slash
function normaliseOrigin(url) {
  return String(url || '').trim().replace(/\/+$/, '');
}

// A version as a declaration states it: a positive integer, nothing else
function parseVersion(version) {
  const s = String(version);
  return /^[1-9][0-9]*$/.test(s) ? parseInt(s, 10) : null;
}

// The 2026 contract, in the shape the orchestrator has always used:
// { <property>: { name, provider: 'yes'|'no', required, mandatory, propagate, default } }
function convertContract(doc) {
  const properties = {};
  const groups = (doc && doc.Properties) || {};

  for (const role of ['Provider', 'Consumer']) {
    for (const p of (groups[role] || [])) {
      if (!p || typeof p.Name !== 'string') continue;

      properties[p.Name] = {
        name: p.Description || p.Name,
        provider: (role === 'Provider') ? 'yes' : 'no',
        required: (p.Mandatory === 'yes') ? 'yes' : 'no',
        mandatory: (p.Mandatory === 'yes') ? 'yes' : 'no',
        propagate: (p.Propagate === 'yes') ? 'yes' : 'no',
        // Read and held; not applied at Bind yet (starting values unchanged)
        default: (p.Default !== undefined) ? String(p.Default) : undefined
      };
    }
  }

  // Held for when Channel conveyance exists; not written to Connections
  const channels = Array.isArray(doc && doc.Channels) ? doc.Channels : [];

  return { properties: properties, channels: channels };
}

function createResolver(opts) {
  const origin = normaliseOrigin(opts.origin);
  const get = opts.get || httpGet;
  const timeout = opts.timeout || 5000;
  const retryGap = (opts.retryGap !== undefined) ? opts.retryGap : 2000;
  const now = opts.now || Date.now;
  const debug = opts.debug || (() => {});

  const surfaces = new Map();   // name -> { etag, versions: Map(n -> status) }
  const absent = new Map();     // name -> kind, until the next revalidate()
  const contracts = new Map();  // 'name:n' -> { properties, channels }
  const retryAt = new Map();    // name -> earliest time to ask again after a transient failure
  const inflight = new Map();   // request key -> promise, so concurrent lookups share one request
  const counts = { surface: 0, notModified: 0, contract: 0, transient: 0 };

  function transient(name, version, detail) {
    counts.transient++;
    retryAt.set(name, now() + retryGap);
    return new ResolveError(UNAVAILABLE, name, version, detail);
  }

  // One request at a time per key; concurrent callers share it
  function once(key, fn) {
    if (inflight.has(key)) return inflight.get(key);

    const p = fn().finally(() => inflight.delete(key));
    inflight.set(key, p);
    return p;
  }

  // Fetch (or revalidate) a name's selection surface
  function fetchSurface(name) {
    return once(name, () => fetchSurfaceNow(name));
  }

  async function fetchSurfaceNow(name) {
    const held = surfaces.get(name);
    const headers = { accept: ACCEPT };

    if (held && held.etag) headers['if-none-match'] = held.etag;

    var res;
    try {
      counts.surface++;
      res = await get(origin + '/' + encodeURIComponent(name), headers, timeout);
    } catch (e) {
      throw transient(name, undefined, e.message);
    }

    if (res.status === 304 && held) {
      counts.notModified++;
      return held;
    }

    if (res.status === 404) {
      surfaces.delete(name);
      absent.set(name, UNREGISTERED);
      throw new ResolveError(UNREGISTERED, name);
    }

    if (res.status !== 200) throw transient(name, undefined, 'HTTP ' + res.status);

    var doc;
    try {
      doc = JSON.parse(res.body);
    } catch (e) {
      throw transient(name, undefined, 'unreadable reply');
    }

    if (!doc || !Array.isArray(doc.versions)) throw transient(name, undefined, 'not a selection surface');

    const versions = new Map();
    for (const v of doc.versions) {
      if (v && Number.isInteger(v.version)) versions.set(v.version, String(v.status || '').toLowerCase());
    }

    const surface = { etag: res.headers.etag, versions: versions };

    surfaces.set(name, surface);
    absent.delete(name);
    retryAt.delete(name);

    return surface;
  }

  // Fetch one version's contract (once)
  function fetchContract(name, n) {
    const key = name + ':' + n;
    const held = contracts.get(key);

    if (held) return Promise.resolve(held);

    return once(key, () => fetchContractNow(name, n, key));
  }

  async function fetchContractNow(name, n, key) {
    var res;
    try {
      counts.contract++;
      res = await get(origin + '/' + encodeURIComponent(name) + ':' + n, { accept: ACCEPT }, timeout);
    } catch (e) {
      throw transient(name, n, e.message);
    }

    if (res.status === 404) throw new ResolveError(NO_VERSION, name, n);
    if (res.status !== 200) throw transient(name, n, 'HTTP ' + res.status);

    var contract;
    try {
      contract = convertContract(JSON.parse(res.body));
    } catch (e) {
      throw transient(name, n, 'unreadable reply');
    }

    contracts.set(key, contract);
    return contract;
  }

  // Resolve a declared name and version. forBind: refuse a Deprecated version.
  async function resolve(name, version, forBind) {
    const n = parseVersion(version);
    if (n === null) throw new ResolveError(NO_VERSION, name, version);

    // Known absent until the next revalidation
    if (absent.has(name)) throw new ResolveError(absent.get(name), name, n);

    var surface = surfaces.get(name);

    if (surface === undefined) {
      // Not held: ask now, unless a transient failure was seen just now
      if ((retryAt.get(name) || 0) > now()) throw new ResolveError(UNAVAILABLE, name, n, 'retrying shortly');
      surface = await fetchSurface(name);
    }

    if (surface.versions.size === 0) throw new ResolveError(UNPUBLISHED, name, n);

    const status = surface.versions.get(n);
    if (status === undefined) throw new ResolveError(NO_VERSION, name, n);

    // §8.6: no new Connection forms at a Deprecated version. Existing
    // Connections are untouched (§6.3), so update() and propagate() still
    // resolve it. Only a published version binds; any status this code does
    // not know is not bound either.
    if (forBind && status !== 'published') {
      if (status === 'deprecated') throw new ResolveError(DEPRECATED, name, n);
      throw new ResolveError(NO_VERSION, name, n, 'status ' + status);
    }

    // Contract not held yet, and a transient failure was seen just now
    if (!contracts.has(name + ':' + n) && (retryAt.get(name) || 0) > now())
      throw new ResolveError(UNAVAILABLE, name, n, 'retrying shortly');

    const contract = await fetchContract(name, n);

    return { properties: contract.properties, channels: contract.channels, status: status };
  }

  // Refresh every held surface, and forget absences so they are asked again.
  // Returns true if anything a build depends on changed.
  async function revalidate() {
    var changed = absent.size > 0;

    absent.clear();
    retryAt.clear();

    await Promise.all([...surfaces.keys()].map(async (name) => {
      const before = surfaces.get(name);
      const was = JSON.stringify([...before.versions]);

      try {
        const after = await fetchSurface(name);
        if (JSON.stringify([...after.versions]) !== was) changed = true;
      } catch (e) {
        // Keep what is held: lifecycle as last held (§7.4). A name now
        // answered as not registered has already been dropped.
        if (e.kind === UNREGISTERED) changed = true;
        debug('Revalidate ' + name + ': ' + e.message);
      }
    }));

    return changed;
  }

  return {
    origin: origin,
    counts: counts,
    // For build(): a published, non-Deprecated version
    bindable: (name, version) => resolve(name, version, true).then((r) => r.properties),
    // For update() and propagate(): any version that exists, Deprecated or not
    properties: (name, version) => resolve(name, version, false).then((r) => r.properties),
    resolve: resolve,
    revalidate: revalidate
  };
}

module.exports = {
  createResolver: createResolver,
  convertContract: convertContract,
  parseVersion: parseVersion,
  normaliseOrigin: normaliseOrigin,
  ResolveError: ResolveError,
  ACCEPT: ACCEPT,
  UNREGISTERED: UNREGISTERED,
  UNPUBLISHED: UNPUBLISHED,
  NO_VERSION: NO_VERSION,
  DEPRECATED: DEPRECATED,
  UNAVAILABLE: UNAVAILABLE
};
