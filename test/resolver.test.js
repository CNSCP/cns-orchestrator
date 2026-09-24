// resolver.test.js - the CP Registry resolver, against recorded canon answers
//
// Run: npm test   (Node's built-in test runner; no network)
//
// Fixtures: test/fixtures/canon.json, recorded from https://cp.cnscp.io with
// Accept: application/cp+json; profile=2026.

'use strict';

const test = require('node:test');
const assert = require('node:assert');

const registry = require('../registry');
const canon = require('./fixtures/canon.json');

const ORIGIN = 'https://cp.example.test';

// A fake Registry serving the recorded answers. Tests can replace answers,
// make it fail, and read what was asked.
function fakeRegistry() {
  const answers = new Map(Object.entries(canon.answers));
  const log = [];
  var failure = null;

  async function get(url, headers) {
    const path = url.slice(ORIGIN.length);
    log.push({ path: path, headers: headers });

    if (failure) throw new Error(failure);

    const a = answers.get(path);
    if (a === undefined) return { status: 404, headers: {}, body: '{"registered":false}' };

    if (a.etag && headers['if-none-match'] === a.etag) return { status: 304, headers: { etag: a.etag }, body: '' };

    return { status: a.status, headers: a.etag ? { etag: a.etag } : {}, body: a.body };
  }

  return {
    get: get,
    log: log,
    set: (path, answer) => answers.set(path, answer),
    fail: (message) => { failure = message; },
    heal: () => { failure = null; },
    asked: (path) => log.filter((e) => e.path === path).length
  };
}

function make(reg, extra) {
  var clock = 1000000;
  const r = registry.createResolver(Object.assign({
    origin: ORIGIN + '/',
    get: reg.get,
    now: () => clock
  }, extra || {}));
  r.advance = (ms) => { clock += ms; };
  return r;
}

async function rejects(promise, kind) {
  await assert.rejects(promise, (e) => {
    assert.strictEqual(e.kind, kind, 'expected "' + kind + '", got "' + e.kind + '": ' + e.message);
    return true;
  });
}

// U1
test('U1 padi.light:1 (grandfathered): four Properties with roles and flags', async () => {
  const r = make(fakeRegistry());
  const p = await r.bindable('padi.light', '1');

  assert.deepStrictEqual(Object.keys(p).sort(), ['cLabel', 'cState', 'sLabel', 'sOut']);
  assert.strictEqual(p.sOut.provider, 'yes');
  assert.strictEqual(p.sLabel.provider, 'yes');
  assert.strictEqual(p.cState.provider, 'no');
  assert.strictEqual(p.cLabel.provider, 'no');

  for (const name in p) {
    assert.strictEqual(p[name].propagate, 'yes');
    assert.strictEqual(p[name].required, 'yes');
    assert.strictEqual(p[name].default, undefined);
  }
});

// U2
test('U2 padi.lighting:2: roles, yes/no flags, Defaults read and held', async () => {
  const r = make(fakeRegistry());
  const p = await r.bindable('padi.lighting', 2);

  assert.deepStrictEqual(Object.keys(p).sort(), ['actual', 'color', 'fade', 'level', 'power']);

  assert.strictEqual(p.level.provider, 'yes');
  assert.strictEqual(p.level.mandatory, 'yes');
  assert.strictEqual(p.level.propagate, 'yes');
  assert.strictEqual(p.level.default, '0');

  assert.strictEqual(p.color.mandatory, 'no');
  assert.strictEqual(p.color.default, undefined);

  assert.strictEqual(p.fade.default, '0');

  assert.strictEqual(p.actual.provider, 'no');
  assert.strictEqual(p.actual.propagate, 'yes');
  assert.strictEqual(p.actual.default, undefined);

  assert.strictEqual(p.power.provider, 'no');
  assert.strictEqual(p.power.propagate, 'no');
  assert.strictEqual(p.power.mandatory, 'no');
});

// U3
test('U3 padi.lighting:1: no Version in its Header; status from the surface', async () => {
  const header = JSON.parse(canon.answers['/padi.lighting:1'].body).Header;
  assert.strictEqual(header.Version, undefined, 'fixture premise: Header has no Version');

  const r = make(fakeRegistry());

  // Resolves by the number asked for, for the paths that serve existing Connections
  const p = await r.properties('padi.lighting', '1');
  assert.ok(p.level);

  const full = await r.resolve('padi.lighting', '1', false);
  assert.strictEqual(full.status, 'deprecated');

  // ... and forms no new Connection
  await rejects(r.bindable('padi.lighting', '1'), registry.DEPRECATED);
});

// U4
test('U4 padi.test.phase1:1: its Channel is parsed (no 406)', async () => {
  const r = make(fakeRegistry());
  const full = await r.resolve('padi.test.phase1', '1', true);

  assert.strictEqual(full.channels.length, 1);
  assert.strictEqual(full.channels[0].Name, 'events');
  assert.strictEqual(full.channels[0].Mode, 'message');
  assert.deepStrictEqual(Object.keys(full.properties).sort(), ['note', 'state']);
});

// U5
test('U5 the surface: versions by number, with status', async () => {
  const reg = fakeRegistry();
  const r = make(reg);

  await r.bindable('padi.lighting', '2');
  await rejects(r.bindable('padi.lighting', '1'), registry.DEPRECATED);
  await rejects(r.bindable('padi.lighting', '3'), registry.NO_VERSION);

  // Versions are never taken by position: "2" is version 2, whatever the order
  const reordered = JSON.parse(canon.answers['/padi.lighting'].body);
  reordered.versions.reverse();
  reg.set('/padi.lighting', { status: 200, etag: '"x"', body: JSON.stringify(reordered) });

  const r2 = make(reg);
  const p = await r2.bindable('padi.lighting', '2');
  assert.ok(p.power);
  await rejects(r2.bindable('padi.lighting', '1'), registry.DEPRECATED);
});

// U6
test('U6 failures are classified, and none is held beyond revalidation', async () => {
  const reg = fakeRegistry();
  const r = make(reg);

  await rejects(r.bindable('nosuch.thing.here', '1'), registry.UNREGISTERED);
  await rejects(r.bindable('padi.appliance', '1'), registry.UNPUBLISHED);
  await rejects(r.bindable('padi.lighting', '3'), registry.NO_VERSION);
  await rejects(r.bindable('padi.light', 'unpublished'), registry.NO_VERSION);

  // Unregistered is held until the next revalidation, then asked again
  const before = reg.asked('/nosuch.thing.here');
  await rejects(r.bindable('nosuch.thing.here', '1'), registry.UNREGISTERED);
  assert.strictEqual(reg.asked('/nosuch.thing.here'), before, 'held until revalidation');

  // The name is registered and published meanwhile
  reg.set('/nosuch.thing.here', canon.answers['/padi.light']);
  reg.set('/nosuch.thing.here:1', canon.answers['/padi.light:1']);
  await r.revalidate();
  const p = await r.bindable('nosuch.thing.here', '1');
  assert.ok(p.sOut);

  // Transient: 5xx, unreadable, unreachable. Never held as an answer.
  const t = make(reg);
  reg.set('/padi.light', { status: 503, body: 'down' });
  await rejects(t.bindable('padi.light', '1'), registry.UNAVAILABLE);

  reg.set('/padi.light', { status: 200, etag: '"bad"', body: '<html>' });
  t.advance(3000);
  await rejects(t.bindable('padi.light', '1'), registry.UNAVAILABLE);

  reg.fail('ECONNREFUSED');
  t.advance(3000);
  await rejects(t.bindable('padi.light', '1'), registry.UNAVAILABLE);

  // Within the retry gap, no request is made
  const asked = reg.asked('/padi.light');
  await rejects(t.bindable('padi.light', '1'), registry.UNAVAILABLE);
  assert.strictEqual(reg.asked('/padi.light'), asked, 'no request storm');

  // Healthy again: the next attempt after the gap resolves
  reg.heal();
  reg.set('/padi.light', canon.answers['/padi.light']);
  t.advance(3000);
  assert.ok((await t.bindable('padi.light', '1')).sOut);
});

// U7
test('U7 revalidation sends If-None-Match; 304 keeps, 200 replaces', async () => {
  const reg = fakeRegistry();
  const r = make(reg);

  await r.bindable('padi.lighting', '2');
  const etag = canon.answers['/padi.lighting'].etag;

  await r.revalidate();
  const last = reg.log.filter((e) => e.path === '/padi.lighting').pop();
  assert.strictEqual(last.headers['if-none-match'], etag);
  assert.strictEqual(r.counts.notModified, 1);

  // Still resolves after the 304
  assert.ok((await r.bindable('padi.lighting', '2')).level);

  // The author Deprecates version 2: the surface changes
  const surface = JSON.parse(canon.answers['/padi.lighting'].body);
  surface.versions.find((v) => v.version === 2).status = 'deprecated';
  reg.set('/padi.lighting', { status: 200, etag: '"changed"', body: JSON.stringify(surface) });

  assert.strictEqual(await r.revalidate(), true, 'reported as a change');
  await rejects(r.bindable('padi.lighting', '2'), registry.DEPRECATED);
  assert.ok((await r.properties('padi.lighting', '2')).level, 'existing Connections still resolve');

  // An outage during revalidation keeps what is held
  reg.fail('ETIMEDOUT');
  await r.revalidate();
  reg.heal();
  assert.ok((await r.properties('padi.lighting', '2')).level);
});

// U8
test('U8 a contract is fetched once, and builds make no request for held Profiles', async () => {
  const reg = fakeRegistry();
  const r = make(reg);

  await r.bindable('padi.light', '1');
  const n = reg.log.length;

  for (var i = 0; i < 20; i++) {
    await r.bindable('padi.light', '1');
    await r.properties('padi.light', 1);
  }
  assert.strictEqual(reg.log.length, n, 'no request on the bind path once held');

  await r.revalidate();
  await r.revalidate();
  assert.strictEqual(reg.asked('/padi.light:1'), 1, 'contract fetched once');
  assert.strictEqual(reg.asked('/padi.light'), 3, 'surface: first fetch plus one per revalidation');
});

test('U8b concurrent lookups of a Profile not yet held share one request each', async () => {
  const reg = fakeRegistry();
  const r = make(reg);

  const all = [];
  for (var i = 0; i < 10; i++) all.push(r.bindable('padi.lighting', '2'), r.properties('padi.lighting', 2));
  await Promise.all(all);

  assert.strictEqual(reg.asked('/padi.lighting'), 1);
  assert.strictEqual(reg.asked('/padi.lighting:2'), 1);
});

// U9
test('U9 origin: trailing slash removed; requests carry the 2026 Accept', async () => {
  assert.strictEqual(registry.normaliseOrigin('https://cp.cnscp.io/'), 'https://cp.cnscp.io');
  assert.strictEqual(registry.normaliseOrigin(' https://cp.cnscp.io// '), 'https://cp.cnscp.io');

  const reg = fakeRegistry();
  const r = make(reg);
  assert.strictEqual(r.origin, ORIGIN);

  await r.bindable('padi.light', '1');
  for (const e of reg.log) assert.strictEqual(e.headers.accept, 'application/cp+json; profile=2026');
  assert.deepStrictEqual(reg.log.map((e) => e.path), ['/padi.light', '/padi.light:1']);
});

// U10
test('U10 the shape handed to update() and propagate() is unchanged, plus mandatory and default', async () => {
  const r = make(fakeRegistry());
  const p = await r.properties('padi.light', '1');

  for (const name in p) {
    assert.deepStrictEqual(Object.keys(p[name]).sort(),
      ['default', 'mandatory', 'name', 'propagate', 'provider', 'required']);
    assert.ok(['yes', 'no'].includes(p[name].provider));
    assert.ok(['yes', 'no'].includes(p[name].propagate));
  }
  assert.strictEqual(p.sOut.name, 'The output state of the controller 0=off, 1=on');
});
