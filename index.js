#!/usr/bin/env node

// index.js - CNS Orchestrator
// Copyright 2025 Padi, Inc. All Rights Reserved.

'use strict';

// Imports

const env = require('dotenv').config();

const etcd = require('etcd3');
const colours = require('colors');

const pack = require('./package.json');

// Errors

const E_OPTION = 'Illegal option';
const E_MISSING = 'Missing argument';
const E_CONFIG = 'Not configured';
const E_CONNECT = 'Not connected';
const E_WATCH = 'Failed to watch';
const E_BUILD = 'Failed to build';
const E_GET = 'Failed to get';
const E_PUT = 'Failed to put';

// Defaults

const defaults = {
  host: '127.0.0.1',
  port: '2379',
  username: 'root',
  password: ''
};

// Configuration

const config = {
  host: process.env.CNS_HOST || defaults.host,
  port: process.env.CNS_PORT || defaults.port,
  username: process.env.CNS_USERNAME || defaults.username,
  password: process.env.CNS_PASSWORD || defaults.password
};

// Options

const options = {
  silent: false,
  debug: false
};

// Local data

var client;
var watcher;

var timer;

// Local functions

// Main entry point
async function main(argv) {
  try {
    // Parse options
    parse(argv);

    // Show welcome
    print('Welcome to CNS-Orchestrator v' + pack.version + '.');

    // Connect to key store
    await connect();
    await watch();

    // Initial rebuild
    rebuild();
  } catch(e) {
    // Failure
    error(e);
    exit(1);
  }
}

// Show usage
function usage() {
  print('Usage: cns-orchestrator [options]\n');

  print('Options:');
  print('  -h, --help                    Output usage information');
  print('  -v, --version                 Output version information');
  print('  -H, --host                    Set network host');
  print('  -P, --port                    Set network port');
  print('  -u, --username                Set network username');
  print('  -p, --password                Set network password');
  print('  -m, --monochrome              Disable console colours');
  print('  -s, --silent                  Disable console output');
  print('  -d, --debug                   Enable debug output\n');

  process.exit();
}

// Show version
function version() {
  print(pack.version);
  process.exit();
}

// Parse arguments
function parse(args) {
  // Process args array
  while (args.length > 0) {
    // Pop next arg
    const arg = args.shift();

    // What option?
    switch(arg) {
      case '-h':
      case '--help':
        // Show usage
        usage();
        break;
      case '-v':
      case '--version':
        // Show version
        version();
        break;
      case '-H':
      case '--host':
        // Network host
        config.host = next(arg, args);
        break;
      case '-P':
      case '--port':
        // Network port
        config.port = next(arg, args);
        break;
      case '-u':
      case '--username':
        // Network user
        config.username = next(arg, args);
        break;
      case '-p':
      case '--password':
        // Network password
        config.password = next(arg, args);
        break;
      case '-m':
      case '--monochrome':
        // No colour mode
        colours.disable();
        break;
      case '-s':
      case '--silent':
        // Silent mode
        options.silent = true;
        break;
      case '-d':
      case '--debug':
        // Debug mode
        options.debug = true;
        break;
      default:
        // Bad option
        throw new Error(E_OPTION + ': ' + arg);
    }
  }
}

// Get next arg
function next(arg, args) {
  // No more args?
  if (args.length === 0)
    throw new Error(E_MISSING + ': ' + arg);

  return args.shift();
}

// Connect client
async function connect() {
  if (client !== undefined) return;

  const port = config.port;
  const host = config.host + (port?(':' + port):'');

  if (!host) throw new Error(E_CONFIG);

  const username = config.username;
  const password = config.password;

  // Client options
  const options = {
    hosts: host
  };

  // Using auth?
  if (username) {
    options.auth = {
      username: username,
      password: password
    };
  }

  // Create client
  print('Network on ' + (username?(username + '@'):'') + host);
  debug('Connecting...');

  client = new etcd.Etcd3(options);
}

// Start watcher
async function watch() {
  if (watcher !== undefined) return;

  // Create watcher
  debug('Watching...');

  watcher = await client.watch()
    .prefix('cns/network/nodes')
    .create()
    .catch((e) => {
      // Failure
      throw new Error(E_WATCH + ': ' + e.message);
    });

  // Attach handlers
  watcher
    .on('connected', () => {
      // Re-connect
      debug('Connected...');
    })
    .on('put', (change) => {
      // Key put
      modified(change);
    })
    .on('delete', (change) => {
      // Key deleted
      deleted(change);
    })
    .on('disconnected', () => {
      // Broken connection
      debug('Disconnected...');
    })
    .on('error', (e) => {
      // Failure
      throw new Error(E_WATCH + ': ' + e.message);
    });
}

// Key has changed
async function modified(change) {
  const created = (change.version === '1');

  const key = change.key.toString();
  const value = change.value.toString();

  debug('PUT ' + key + ' = ' + value);

  const parts = key.split('/');

  // cns/network/nodes/{node}/contexts/{context}/{role}/{profile}/connections/{connection}/properties
  switch (parts[6]) {
    case 'provider':
    case 'consumer':
      // Capability
      if (created) {
        switch (parts[8]) {
          case 'version':
            break;
          default:
            return;
        }
      } else {
        switch (parts[8]) {
          case 'version':
          case 'scope':
            break;
          case 'connections':
            if (parts[10] === 'properties')
              await update(key, value);
            return;
          case 'properties':
            propagate(key, value);
            return;
          default:
            return;
        }
      }
      rebuild();
      break;
  }
}

// Update connection property
async function update(key, value) {
  const parts = key.split('/');

  // cns/network/nodes/{node}/contexts/{context}/{role}/{profile}/connections/{connection}/properties/{property}
  const node = parts[3];
  const context = parts[5];
  const role = parts[6];
  const profile = parts[7];
  const connection = parts[9];
  const property = parts[11];

  //
  const version = await get('cns/network/nodes/' + node + '/contexts/' + context + '/' + role + '/' + profile + '/version');
  if (version === null) return;

  //
  const provider = await get('cns/network/profiles/' + profile + '/versions/version' + version + '/properties/' + property + '/provider');
  if (provider === null) return;

  switch (role) {
    case 'provider':
      if (provider !== 'yes') return;
      break;
    case 'consumer':
      if (provider === 'yes') return;
      break;
    default:
      return;
  }

  //
  const anti = (role === 'provider')?'consumer':'provider';

  const other = await get('cns/network/nodes/' + node + '/contexts/' + context + '/' + role + '/' + profile + '/connections/' + connection + '/' + anti);
  if (other === null) return;

  //
  debug('UPDATE ' + property + ' = ' + value);
  await put(other + '/' + anti + '/' + profile + '/connections/' + connection + '/properties/' + property, value);
}

//
async function propagate(key, value) {
//  const parts = key.split('/');
//  const name = parts[11];

//  console.log('** PROPAGATE ' + name + ' = ' + value);
}

//
function deleted(change) {
  const key = change.key.toString();
  const value = change.value.toString();

  debug('DEL ' + key);

  const parts = key.split('/');

  switch (parts[6]) {
    case 'provider':
    case 'consumer':
      switch (parts[8]) {
        case 'connections':
          return;
      }
//      rebuild();
      break;
  }
}

//
function rebuild() {
  cancel();

  timer = setTimeout(() => {
    timer = undefined;
    build();
  }, 1000);
}

//
function cancel() {
  if (timer === undefined) return;

  clearTimeout(timer);
  timer = undefined;
}

//
async function build() {
  //
  const mode = await get('cns/network/orchestrator');
  if (mode !== 'nodes' && mode !== 'contexts') return;

  debug('Building...');

  // Read network cache
  const network = await client.getAll()
    .prefix('cns/network')
    .strings()
    .catch((e) => {
      throw new Error(E_BUILD + ': ' + e.message);
    });

  const add = [];
  const nodes = filter(network, 'cns/network/nodes/*/name');

  for (const key in nodes) {
    const parts = key.split('/');
    const node = parts[3];

    debug('  Node ' + node + '...');

    const contexts = filter(network, 'cns/network/nodes/' + node + '/contexts/*/name');

    for (const key in contexts) {
      const parts = key.split('/');
      const context = parts[5];

      debug('    Context ' + context + '...');

      const provider = filter(network, 'cns/network/nodes/' + node + '/contexts/' + context + '/provider/*/version');
      const consumer = filter(network, 'cns/network/nodes/' + node + '/contexts/' + context + '/consumer/*/version');

      for (const key in provider) {
        const parts = key.split('/');
        const profile = parts[7];
        const version = provider[key];

        debug('      Provides ' + profile + '...');

        connections(network, mode, 'cns/network/nodes/' + node + '/contexts/' + context, profile, version, add);
      }

      for (const key in consumer) {
        const parts = key.split('/');
        const profile = parts[7];
        const version = consumer[key];

        debug('      Consumes ' + profile + '...');
      }
    }
  }

  var idp = 0;//0000000 + ((Math.random() * 9999999) | 0);
  var idc = 0;//10000000 + ((Math.random() * 9999999) | 0);

  for (const c of add) {
    const provider = filter(network, c.provider + '/provider/' + c.profile + '/connections/*/consumer');
    const consumer = filter(network, c.consumer + '/consumer/' + c.profile + '/connections/*/provider');

    var addp = true;
    var addc = true;

    for (const key in provider) {
      const parts = key.split('/');
      const id = Number(parts[9].split(/(\d+)/)[1] || 0);

      if (provider[key] === c.consumer)
        addp = false;

      if (idp < id) idp = id;
    }

    for (const key in consumer) {
      const parts = key.split('/');
      const id = Number(parts[9].split(/(\d+)/)[1] || 0);

      if (consumer[key] === c.provider)
        addc = false;

      if (idc < id) idc = id;
    }

    idp++;
    idc++;

    var properties = {};

    if (addp || addc) {
      const propsp = filter(network, c.provider + '/provider/' + c.profile + '/properties/*');
      const propsc = filter(network, c.consumer + '/consumer/' + c.profile + '/properties/*');

      for (const key in propsp) {
        const parts = key.split('/');
        const name = parts[9];

        properties[name] = propsp[key];
      }

      for (const key in propsc) {
        const parts = key.split('/');
        const name = parts[9];

        properties[name] = propsc[key];
      }
    }

    if (addp) {
      debug('Creating provider connection ' + idp);

      const ns = c.provider + '/provider/' + c.profile + '/connections/connection' + idp + '/';

//      await put(ns + 'provider', c.provider);
      await put(ns + 'consumer', c.consumer);

      for (const name in properties)
        await put(ns + 'properties/' + name, properties[name]);
    }

    if (addc) {
      debug('Creating consumer connection ' + idc);

      const ns = c.consumer + '/consumer/' + c.profile + '/connections/connection' + idc + '/';

      await put(ns + 'provider', c.provider);
//      await put(ns + 'consumer', c.consumer);

      for (const name in properties)
        await put(ns + 'properties/' + name, properties[name]);
    }
  }
}

//
function connections(network, mode, provider, profile, version, add) {
  const nodes = filter(network, 'cns/network/nodes/*/name');

  for (const key in nodes) {
    const parts = key.split('/');
    const node = parts[3];

    const contexts = filter(network, 'cns/network/nodes/' + node + '/contexts/*/name');

    for (const key in contexts) {
      const parts = key.split('/');
      const context = parts[5];

      const consumer = filter(network, 'cns/network/nodes/' + node + '/contexts/' + context + '/consumer/' + profile + '/version');

      for (const key in consumer) {
        // Same profile version?
        if (version === consumer[key]) {
          // Add possible connection
          add.push({
            provider: provider,
            consumer: 'cns/network/nodes/' + node + '/contexts/' + context,
            profile: profile,
            version: version
          });
        }
      }
    }
  }
}

// Get key value
async function get(key) {
  if (client === undefined)
    throw new Error(E_CONNECT);

  return await client.get(key)
    .string()
    .catch((e) => {
      // Failure
      throw new Error(E_GET + ': ' + e.message);
    });
}

// Put key value
async function put(key, value) {
  if (client === undefined)
    throw new Error(E_CONNECT);

  await client.put(key)
    .value(value)
    .catch((e) => {
      // Failure
      throw new Error(E_PUT + ': ' + e.message);
    });
}

// Filter keys
function filter(keys, filter) {
  const filters = filter.split('/');
  const result = {};

  for (const key in keys) {
    if (match(key, filters))
      result[key] = keys[key];
  }
  return result;
}

// Match key to filters
function match(key, filters) {
  const keys = key.split('/');

  if (keys.length === filters.length) {
    for (var n = 0; n < keys.length; n++)
      if (!wildcard(keys[n], filters[n])) return false;

    return true;
  }
  return false;
}

// Wilcard match
function wildcard(str, filter) {
  var esc = (str) => str.replace(/([.*+?^=!:${}()|\[\]\/\\])/g, '\\$1');
  return new RegExp('^' + filter.split('*').map(esc).join('.*') + '$').test(str);
}

// Cancel watcher
async function unwatch() {
  if (watcher === undefined) return;

  debug('Unwatching...');
  await watcher.cancel();

  watcher = undefined;
}

// Disconnect client
async function disconnect() {
  if (client === undefined) return;

  debug('Disconnecting...');
  await client.close();

  client = undefined;
}

// Terminate application
async function exit(code) {
  cancel();

  await unwatch();
  await disconnect();

  process.exit(code);
}

// Log text to console
function print(text) {
  if (!options.silent)
    console.log(text.green);
}

// Log debug to console
function debug(text) {
  if (options.debug)
    console.debug(text.magenta);
}

// Log error to console
function error(e) {
  console.error(e.message.red);
  debug(e.stack);
}

// Catch terminate signal
process.on('SIGINT', () => {
  print('\rAborted.');
  exit(1);
});

// Start application
main(process.argv.slice(2));
