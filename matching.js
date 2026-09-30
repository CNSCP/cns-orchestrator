// matching.js - which providers and consumers could be connected
//
// The orchestrator keeps every key of the realm in memory. Finding the
// possible connections used to mean scanning all of those keys with a
// wildcard filter for every provider, system, node and context, so the work
// grew as (providers x systems x keys) and a bind took seconds once a realm
// held a few hundred keys. Here the keys are read once into an index and the
// candidates are looked up in it.
//
// Pure functions of the key/value store: no etcd, no timers, no I/O.

'use strict';

const SEP = '\u0001';

// Is valid mode
function isValidMode(mode) {
  switch (mode) {
    case 'allsystems':
    case 'bysystem':
      return true;
  }
  return false;
}

// Index the store in one pass
//
// cache: { key: value } for the whole realm
//
// Returns:
//   networks   systems that have a name key
//   providers  [{ net, node, ctx, profile, version }] on a named system/node/context
//   consumers  Map 'net|ctx|profile(lowercase)|version' -> [node]
//   conns      Map 'capability path(lowercase)|consumer or provider' -> [{ id, value }]
function buildIndex(cache) {
  const networks = [];
  const nodeNames = new Map();
  const ctxNames = new Map();
  const provs = [];
  const cons = [];
  const conns = new Map();

  const add = (map, key, item) => {
    const list = map.get(key);
    if (list) list.push(item); else map.set(key, [item]);
  };

  for (const key in cache) {
    const p = key.split('/');
    if (p[0] !== 'cns') continue;

    const n = p.length;

    if (n === 3 && p[2] === 'name') {
      networks.push(p[1]);
    } else if (n === 5 && p[2] === 'nodes' && p[4] === 'name') {
      add(nodeNames, p[1], p[3]);
    } else if (n === 7 && p[2] === 'nodes' && p[4] === 'contexts' && p[6] === 'name') {
      add(ctxNames, p[1] + SEP + p[3], p[5]);
    } else if (n === 9 && p[2] === 'nodes' && p[4] === 'contexts' && p[8] === 'version' &&
               (p[6] === 'provider' || p[6] === 'consumer')) {
      (p[6] === 'provider' ? provs : cons).push({
        net: p[1], node: p[3], ctx: p[5], profile: p[7], version: cache[key]
      });
    } else if (n === 11 && p[2] === 'nodes' && p[4] === 'contexts' && p[8] === 'connections' &&
               (p[6] === 'provider' || p[6] === 'consumer') &&
               (p[10] === 'consumer' || p[10] === 'provider')) {
      add(conns, p.slice(0, 8).join('/').toLowerCase() + '|' + p[10], { id: p[9], value: cache[key] });
    }
  }

  // A capability counts only when its system, node and context each have a name
  const netSet = new Set(networks);
  const nodeSet = new Set();
  const ctxSet = new Set();

  for (const [net, list] of nodeNames)
    if (netSet.has(net)) for (const node of list) nodeSet.add(net + SEP + node);

  for (const [nk, list] of ctxNames)
    if (nodeSet.has(nk)) for (const ctx of list) ctxSet.add(nk + SEP + ctx);

  const consumers = new Map();

  for (const c of cons) {
    if (!ctxSet.has(c.net + SEP + c.node + SEP + c.ctx)) continue;
    add(consumers, c.net + SEP + c.ctx + SEP + c.profile.toLowerCase() + SEP + c.version, c.node);
  }

  const providers = provs.filter((p) => ctxSet.has(p.net + SEP + p.node + SEP + p.ctx));

  return { networks, providers, consumers, conns };
}

// Candidate connections: [{ provider, consumer, profile, version }]
//
// A provider matches a consumer of the same profile (case-insensitive), the
// same version and the same context. The system's `orchestrator` mode decides
// where consumers are looked for: 'allsystems' the whole realm, 'bysystem'
// only the provider's own system. A system without a valid mode is skipped.
function candidates(cache, idx) {
  const add = [];

  for (const p of idx.providers) {
    const mode = cache['cns/' + p.net + '/orchestrator'];
    if (!isValidMode(mode)) continue;

    const nets = mode === 'allsystems' ? idx.networks : [p.net];
    const provider = 'cns/' + p.net + '/nodes/' + p.node + '/contexts/' + p.ctx;

    for (const cn of nets) {
      const list = idx.consumers.get(cn + SEP + p.ctx + SEP + p.profile.toLowerCase() + SEP + p.version);
      if (!list) continue;

      for (const node of list) {
        add.push({
          provider: provider,
          consumer: 'cns/' + cn + '/nodes/' + node + '/contexts/' + p.ctx,
          profile: p.profile,
          version: p.version
        });
      }
    }
  }
  return add;
}

// Existing connections of one side of a candidate: [{ id, value }]
//
// side 'provider': the provider capability's connections, each recording its consumer
// side 'consumer': the consumer capability's connections, each recording its provider
function existing(idx, candidate, side) {
  const path = side === 'provider'
    ? candidate.provider + '/provider/' + candidate.profile
    : candidate.consumer + '/consumer/' + candidate.profile;

  return idx.conns.get(path.toLowerCase() + '|' + (side === 'provider' ? 'consumer' : 'provider')) || [];
}

// A live index of a capability's properties and connection keys
//
// The orchestrator reads these three ways, each of which used to scan every
// key of the realm with a wildcard filter: the properties a new connection
// starts with, the properties to catch up after it is written, and the
// connections to update when a property changes. The scan made each of them
// cost as much as the realm is big.
//
// This index holds only keys, never values, so a reader asks the store for the
// value at the time it reads, and sees writes that landed since. It is kept up
// to date from the places the store changes (set, del, reset).
//
// A capability path is the first eight segments of a key, compared without
// regard to case, as the wildcard filter did.
class CapIndex {
  constructor() { this.reset({}); }

  // Start again from a whole store
  reset(cache) {
    this.props = new Map();
    this.conns = new Map();
    for (const key in cache) this.set(key);
  }

  // Note that a key exists
  set(key) { this._edit(key, true); }

  // Note that a key is gone
  del(key) { this._edit(key, false); }

  // Keys of a capability's properties: cns/<s>/nodes/<n>/contexts/<c>/<role>/<profile>/properties/*
  properties(capability) {
    const keys = this.props.get(capability.toLowerCase());
    return keys ? Array.from(keys) : [];
  }

  // Keys of a capability's connections recording `opposite` ('consumer' or 'provider')
  connections(capability, opposite) {
    const keys = this.conns.get(capability.toLowerCase() + '|' + opposite.toLowerCase());
    return keys ? Array.from(keys) : [];
  }

  _edit(key, present) {
    const p = key.split('/');
    if (p[0] !== 'cns' || p[2] !== 'nodes' || p[4] !== 'contexts') return;
    if (p[6] !== 'provider' && p[6] !== 'consumer') return;

    var map, id;

    if (p.length === 10 && p[8] === 'properties') {
      map = this.props; id = p.slice(0, 8).join('/').toLowerCase();
    } else if (p.length === 11 && p[8] === 'connections' && (p[10] === 'consumer' || p[10] === 'provider')) {
      map = this.conns; id = p.slice(0, 8).join('/').toLowerCase() + '|' + p[10];
    } else {
      return;
    }

    var set = map.get(id);

    if (present) {
      if (set === undefined) map.set(id, set = new Set());
      set.add(key);
    } else if (set !== undefined) {
      set.delete(key);
      if (set.size === 0) map.delete(id);
    }
  }
}

module.exports = { isValidMode, buildIndex, candidates, existing, CapIndex };
