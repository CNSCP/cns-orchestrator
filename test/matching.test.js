// matching.test.js - indexed matching against the scan it replaced
//
// Run: npm test   (no network, no etcd)
//
// `scan` below is the matching code of orchestrator 0.1.6, kept verbatim as an
// oracle: a wildcard filter over every key of the realm for each provider,
// system and node. The index must find the same candidate connections and the
// same existing connections, with one deliberate difference (see "names that
// differ only by case").

'use strict';

const test = require('node:test');
const assert = require('node:assert');

const matching = require('../matching');

// ---- The oracle: 0.1.6 matching, verbatim -------------------------------

function filter(keys, filter) {
  const result = {};
  const filters = filter.split('/');
  for (const key in keys) if (compare(key, filters)) result[key] = keys[key];
  return result;
}
function compare(key, filters) {
  const keys = key.split('/');
  if (keys.length === filters.length) {
    for (var n = 0; n < keys.length; n++) if (!match(keys[n], filters[n])) return false;
    return true;
  }
  return false;
}
function match(text, filter) {
  const esc = (s) => s.replace(/([.*+?^=!:${}()|\[\]\/\\])/g, '\\$1');
  return new RegExp('^' + filter.split('*').map(esc).join('.*') + '$', 'i').test(text);
}

function scan(cache) {
  const add = [];
  const isValidMode = matching.isValidMode;
  const networks = filter(cache, 'cns/*/name');

  for (const key in networks) {
    const network = key.split('/')[1];
    const ns1 = 'cns/' + network;
    const mode = cache[ns1 + '/orchestrator'];
    if (!isValidMode(mode)) continue;

    const nodes = filter(cache, ns1 + '/nodes/*/name');
    for (const key in nodes) {
      const node = key.split('/')[3];
      const ns2 = ns1 + '/nodes/' + node;
      const contexts = filter(cache, ns2 + '/contexts/*/name');
      for (const key in contexts) {
        const context = key.split('/')[5];
        const ns3 = ns2 + '/contexts/' + context;
        const provider = filter(cache, ns3 + '/provider/*/version');
        for (const key in provider) {
          const profile = key.split('/')[7];
          const version = provider[key];
          const pc = 'cns/' + network + '/nodes/' + node + '/contexts/' + context;
          if (mode === 'allsystems') {
            for (const k in filter(cache, 'cns/*/name')) bysystem(k.split('/')[1], pc, profile, version, context, add);
          } else {
            bysystem(network, pc, profile, version, context, add);
          }
        }
      }
    }
  }
  return add;
}

function bysystem(network, provider, profile, version, scope, add) {
  const nodes = filter(cache_, 'cns/' + network + '/nodes/*/name');
  for (const key in nodes) {
    const node = key.split('/')[3];
    const contexts = filter(cache_, 'cns/' + network + '/nodes/' + node + '/contexts/*/name');
    for (const key in contexts) {
      const context = key.split('/')[5];
      if (context === scope) {
        const consumer = 'cns/' + network + '/nodes/' + node + '/contexts/' + context;
        const capabilities = filter(cache_, consumer + '/consumer/' + profile + '/version');
        for (const key in capabilities) {
          if (capabilities[key] === version) add.push({ provider, consumer, profile, version });
        }
      }
    }
  }
}

// bysystem() reads the store being scanned
var cache_ = {};
function oracle(cache) { cache_ = cache; return scan(cache); }

function oracleExisting(cache, c) {
  const provider = filter(cache, c.provider + '/provider/' + c.profile + '/connections/*/consumer');
  const consumer = filter(cache, c.consumer + '/consumer/' + c.profile + '/connections/*/provider');
  const out = { provider: [], consumer: [] };
  for (const key in provider) out.provider.push({ id: key.split('/')[9], value: provider[key] });
  for (const key in consumer) out.consumer.push({ id: key.split('/')[9], value: consumer[key] });
  return out;
}

// ---- Helpers ------------------------------------------------------------

const sortKey = (c) => [c.provider, c.consumer, c.profile, c.version].join('|');
const canon = (list) => list.map(sortKey).sort();

// Declare systems in a store
function realm() {
  const cache = {};
  const r = {
    cache,
    system(id, mode) {
      cache['cns/' + id + '/name'] = id;
      if (mode !== undefined) cache['cns/' + id + '/orchestrator'] = mode;
      return r;
    },
    node(sys, node) { cache['cns/' + sys + '/nodes/' + node + '/name'] = node; return r; },
    context(sys, node, ctx) { cache['cns/' + sys + '/nodes/' + node + '/contexts/' + ctx + '/name'] = ctx; return r; },
    cap(sys, node, ctx, role, profile, version) {
      cache['cns/' + sys + '/nodes/' + node + '/contexts/' + ctx + '/' + role + '/' + profile + '/version'] = version;
      return r;
    },
    conn(sys, node, ctx, role, profile, id, other) {
      const base = 'cns/' + sys + '/nodes/' + node + '/contexts/' + ctx + '/' + role + '/' + profile;
      cache[base + '/connections/' + id + '/' + (role === 'provider' ? 'consumer' : 'provider')] = other;
      return r;
    },
    // a whole system with one node and one context
    full(sys, mode, ctx, caps) {
      r.system(sys, mode).node(sys, 'n').context(sys, 'n', ctx);
      for (const [role, profile, version] of caps) r.cap(sys, 'n', ctx, role, profile, version);
      return r;
    }
  };
  return r;
}

function same(cache) {
  const idx = matching.buildIndex(cache);
  assert.deepStrictEqual(canon(matching.candidates(cache, idx)), canon(oracle(cache)));
  return idx;
}

// ---- Scenarios ----------------------------------------------------------

test('one provider and one consumer in a system match', () => {
  const r = realm().full('a', 'allsystems', 'x', [['provider', 'padi.light', '1'], ['consumer', 'padi.light', '1']]);
  // same node, same context: the provider can match its own system's consumer
  const idx = same(r.cache);
  assert.strictEqual(matching.candidates(r.cache, idx).length, 1);
});

test('allsystems matches across systems, bysystem does not', () => {
  const all = realm()
    .full('a', 'allsystems', 'x', [['provider', 'padi.light', '1']])
    .full('b', 'allsystems', 'x', [['consumer', 'padi.light', '1']]);
  assert.strictEqual(matching.candidates(all.cache, same(all.cache)).length, 1);

  const by = realm()
    .full('a', 'bysystem', 'x', [['provider', 'padi.light', '1']])
    .full('b', 'bysystem', 'x', [['consumer', 'padi.light', '1']]);
  assert.strictEqual(matching.candidates(by.cache, same(by.cache)).length, 0);
});

test('the provider system\'s mode decides, not the consumer system\'s', () => {
  const r = realm()
    .full('a', 'allsystems', 'x', [['provider', 'padi.light', '1']])
    .full('b', 'bysystem', 'x', [['consumer', 'padi.light', '1']]);
  assert.strictEqual(matching.candidates(r.cache, same(r.cache)).length, 1);

  const s = realm()
    .full('a', 'bysystem', 'x', [['provider', 'padi.light', '1']])
    .full('b', 'allsystems', 'x', [['consumer', 'padi.light', '1']]);
  assert.strictEqual(matching.candidates(s.cache, same(s.cache)).length, 0);
});

test('version must be equal (no ranges, no prefixes)', () => {
  const r = realm()
    .full('a', 'allsystems', 'x', [['provider', 'padi.lighting', '2'], ['provider', 'padi.lighting', '2']])
    .full('b', 'allsystems', 'x', [['consumer', 'padi.lighting', '1']])
    .full('c', 'allsystems', 'x', [['consumer', 'padi.lighting', '2']])
    .full('d', 'allsystems', 'x', [['consumer', 'padi.lighting', '20']]);
  const c = matching.candidates(r.cache, same(r.cache));
  assert.ok(c.every((m) => m.consumer.startsWith('cns/c/')));
});

test('context must be equal', () => {
  const r = realm()
    .full('a', 'allsystems', 'x', [['provider', 'padi.light', '1']])
    .full('b', 'allsystems', 'y', [['consumer', 'padi.light', '1']]);
  assert.strictEqual(matching.candidates(r.cache, same(r.cache)).length, 0);
});

test('a system with no mode, or an unknown mode, is skipped and does not stop the others', () => {
  const r = realm()
    .full('a', undefined, 'x', [['provider', 'padi.light', '1']])
    .full('b', 'nonsense', 'x', [['provider', 'padi.light', '1']])
    .full('c', 'allsystems', 'x', [['provider', 'padi.light', '1'], ['consumer', 'padi.light', '1']]);
  const c = matching.candidates(r.cache, same(r.cache));
  assert.ok(c.length >= 1 && c.every((m) => m.provider.startsWith('cns/c/')));
});

test('capabilities on a node or context with no name, or a system with no name, are ignored', () => {
  const r = realm().full('a', 'allsystems', 'x', [['provider', 'padi.light', '1']]);
  // consumer on a node that has no name key
  r.cache['cns/a/nodes/ghost/contexts/x/name'] = 'x';
  r.cache['cns/a/nodes/ghost/contexts/x/consumer/padi.light/version'] = '1';
  // consumer on a context that has no name key
  r.cache['cns/a/nodes/n/contexts/z/consumer/padi.light/version'] = '1';
  // a whole system with no name key
  r.cache['cns/noname/orchestrator'] = 'allsystems';
  r.cache['cns/noname/nodes/n/name'] = 'n';
  r.cache['cns/noname/nodes/n/contexts/x/name'] = 'x';
  r.cache['cns/noname/nodes/n/contexts/x/consumer/padi.light/version'] = '1';
  assert.strictEqual(matching.candidates(r.cache, same(r.cache)).length, 0);
});

test('several nodes and several consumers of one provider', () => {
  const r = realm().system('a', 'allsystems').system('b', 'allsystems');
  for (const n of ['n1', 'n2', 'n3']) {
    r.node('a', n).context('a', n, 'x').cap('a', n, 'x', 'provider', 'padi.light', '1').cap('a', n, 'x', 'consumer', 'padi.light', '1');
    r.node('b', n).context('b', n, 'x').cap('b', n, 'x', 'consumer', 'padi.light', '1');
  }
  assert.strictEqual(matching.candidates(r.cache, same(r.cache)).length, 3 * 6);
});

test('keys that are not capabilities (properties, connections, other depths) do not disturb it', () => {
  const r = realm().full('a', 'allsystems', 'x', [['provider', 'padi.light', '1'], ['consumer', 'padi.light', '1']]);
  r.cache['cns/a/nodes/n/contexts/x/provider/padi.light/properties/sOut'] = '1';
  r.cache['cns/a/nodes/n/contexts/x/provider/padi.light/scope'] = 'x';
  r.cache['cns/a/nodes/n/name/extra'] = 'x';
  r.cache['cns/a/extra/deep/key/with/many/parts/in/it/version'] = '1';
  r.cache['other/a/name'] = 'a';
  r.cache['cns'] = 'x';
  same(r.cache);
});

test('empty store', () => {
  const idx = same({});
  assert.deepStrictEqual(matching.candidates({}, idx), []);
});

test('existing connections are found, from either side', () => {
  const r = realm()
    .full('a', 'allsystems', 'x', [['provider', 'padi.light', '1']])
    .full('b', 'allsystems', 'x', [['consumer', 'padi.light', '1']]);
  const pc = 'cns/a/nodes/n/contexts/x';
  const cc = 'cns/b/nodes/n/contexts/x';
  r.conn('a', 'n', 'x', 'provider', 'padi.light', 'id1', cc);
  r.conn('b', 'n', 'x', 'consumer', 'padi.light', 'id1', pc);
  r.conn('a', 'n', 'x', 'provider', 'padi.light', 'id2', 'cns/elsewhere/nodes/n/contexts/x');

  const idx = matching.buildIndex(r.cache);
  const [c] = matching.candidates(r.cache, idx);
  const want = oracleExisting(r.cache, c);

  assert.deepStrictEqual(matching.existing(idx, c, 'provider'), want.provider);
  assert.deepStrictEqual(matching.existing(idx, c, 'consumer'), want.consumer);
  assert.strictEqual(matching.existing(idx, c, 'provider').length, 2);
  assert.strictEqual(matching.existing(idx, c, 'consumer').length, 1);
});

test('no existing connections gives an empty list, not undefined', () => {
  const r = realm().full('a', 'allsystems', 'x', [['provider', 'padi.light', '1'], ['consumer', 'padi.light', '1']]);
  const idx = matching.buildIndex(r.cache);
  const [c] = matching.candidates(r.cache, idx);
  assert.deepStrictEqual(matching.existing(idx, c, 'provider'), []);
  assert.deepStrictEqual(matching.existing(idx, c, 'consumer'), []);
});

test('profile names differing only by case still match each other (as before)', () => {
  const r = realm()
    .full('a', 'allsystems', 'x', [['provider', 'Padi.Light', '1']])
    .full('b', 'allsystems', 'x', [['consumer', 'padi.light', '1']]);
  assert.strictEqual(matching.candidates(r.cache, same(r.cache)).length, 1);
});

// The one deliberate difference from 0.1.6. The scan compared names with
// case-insensitive wildcard patterns, so a consumer on a SYSTEM or CONTEXT
// whose name differed only by case could be paired with a provider, and the
// candidate then named a path that does not exist. The index compares system,
// node and context names exactly. Profile names stay case-insensitive.
test('names that differ only by case: the index does not invent pairs the scan did', () => {
  const r = realm()
    .full('a', 'allsystems', 'x', [['provider', 'padi.light', '1']])
    .full('Ab', 'allsystems', 'x', [['consumer', 'padi.light', '1']])
    .full('ab', 'allsystems', 'x', [['consumer', 'padi.light', '1']]);

  const idx = matching.buildIndex(r.cache);
  const got = canon(matching.candidates(r.cache, idx));
  const old = canon(oracle(r.cache));

  // every pair the index finds, the scan found
  for (const g of got) assert.ok(old.includes(g), 'index found a pair the scan did not: ' + g);
  assert.strictEqual(got.length, 2);
});

// ---- Seeded random realms ------------------------------------------------

function rng(seed) {
  let s = seed >>> 0;
  return () => { s = (s * 1664525 + 1013904223) >>> 0; return s / 4294967296; };
}

function randomRealm(seed) {
  const rand = rng(seed);
  const pick = (a) => a[Math.floor(rand() * a.length)];
  const r = realm();
  const modes = ['allsystems', 'allsystems', 'bysystem', 'bysystem', 'bogus', undefined];
  const profiles = ['padi.light', 'padi.lighting', 'padi.sensor'];
  const versions = ['1', '2', '3'];
  const ctxs = ['x', 'y', 'z'];
  const nsys = 1 + Math.floor(rand() * 6);

  for (let s = 0; s < nsys; s++) {
    const sys = 'sys' + s;
    if (rand() < 0.9) r.system(sys, pick(modes)); else r.cache['cns/' + sys + '/orchestrator'] = pick(modes); // sometimes unnamed
    const nn = 1 + Math.floor(rand() * 3);
    for (let n = 0; n < nn; n++) {
      const node = 'n' + n;
      if (rand() < 0.95) r.node(sys, node);
      for (const ctx of ctxs) {
        if (rand() < 0.6) {
          if (rand() < 0.95) r.context(sys, node, ctx);
          for (const role of ['provider', 'consumer']) {
            for (const profile of profiles) {
              if (rand() < 0.35) r.cap(sys, node, ctx, role, profile, pick(versions));
            }
          }
        }
      }
    }
  }
  return r.cache;
}

test('200 seeded random realms: same candidates as the scan', () => {
  for (let seed = 1; seed <= 200; seed++) {
    const cache = randomRealm(seed);
    const idx = matching.buildIndex(cache);
    assert.deepStrictEqual(canon(matching.candidates(cache, idx)), canon(oracle(cache)), 'seed ' + seed);
  }
});

test('random realms: existing connections match the scan for every candidate', () => {
  for (let seed = 1; seed <= 60; seed++) {
    const cache = randomRealm(seed);
    const rand = rng(seed * 7919);
    const idx0 = matching.buildIndex(cache);

    // record connections for some candidates, on one side, the other, both, or with the wrong peer
    for (const c of matching.candidates(cache, idx0)) {
      const roll = rand();
      const pbase = c.provider + '/provider/' + c.profile + '/connections/id' + Math.floor(rand() * 100);
      const cbase = c.consumer + '/consumer/' + c.profile + '/connections/id' + Math.floor(rand() * 100);
      if (roll < 0.25) cache[pbase + '/consumer'] = c.consumer;
      else if (roll < 0.5) cache[cbase + '/provider'] = c.provider;
      else if (roll < 0.75) { cache[pbase + '/consumer'] = c.consumer; cache[cbase + '/provider'] = c.provider; }
      else cache[pbase + '/consumer'] = 'cns/other/nodes/n/contexts/x';
    }

    const idx = matching.buildIndex(cache);
    const cands = matching.candidates(cache, idx);
    assert.deepStrictEqual(canon(cands), canon(oracle(cache)), 'seed ' + seed);

    for (const c of cands) {
      const want = oracleExisting(cache, c);
      const sortId = (l) => l.slice().sort((a, b) => a.id.localeCompare(b.id) || String(a.value).localeCompare(String(b.value)));
      assert.deepStrictEqual(sortId(matching.existing(idx, c, 'provider')), sortId(want.provider), 'seed ' + seed);
      assert.deepStrictEqual(sortId(matching.existing(idx, c, 'consumer')), sortId(want.consumer), 'seed ' + seed);
    }
  }
});

test('a realm of 3,000 keys is indexed in well under a second', () => {
  const r = realm();
  for (let s = 0; s < 100; s++) {
    r.full('s' + s, 'allsystems', 'x', [['provider', 'padi.light', '1'], ['consumer', 'padi.light', '2'], ['consumer', 'padi.sensor', '1']]);
    for (let k = 0; k < 20; k++) r.cache['cns/s' + s + '/nodes/n/contexts/x/provider/padi.light/properties/p' + k] = String(k);
  }
  const t0 = Date.now();
  const idx = matching.buildIndex(r.cache);
  matching.candidates(r.cache, idx);
  assert.ok(Date.now() - t0 < 1000, 'took ' + (Date.now() - t0) + ' ms');
});
