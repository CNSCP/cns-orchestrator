#!/usr/bin/env node

// index.js - CNS Orchestrator
// Copyright 2025 Padi, Inc. All Rights Reserved.

'use strict';

// Imports

const env = require('dotenv').config();

const etcd = require('etcd3');
const short = require('short-uuid');
const colours = require('colors');

const pack = require('./package.json');

// Errors

const E_OPTION = 'Illegal option';
const E_MISSING = 'Missing argument';
const E_CONFIG = 'Not configured';
const E_CONNECT = 'Not connected';
const E_WATCH = 'Failed to watch';
const E_ALL = 'Failed to get all';
const E_GET = 'Failed to get';
const E_PUT = 'Failed to put';

// Defaults

const defaults = {
  host: '127.0.0.1',
  port: '2379',
  username: '',
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

var cache;
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
}

// Show version
function version() {
  print(pack.version);
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
        exit();
        break;
      case '-v':
      case '--version':
        // Show version
        version();
        exit();
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
  // Disconnect previous
  await disconnect();

  const host = config.host;
  const port = config.port;

  if (!host) throw new Error(E_CONFIG);

  const username = config.username;
  const password = config.password;

  // Client options
  const options = {
    hosts: host + (port?(':' + port):'')
  };

  // Using auth?
  if (username !== '') {
    options.auth = {
      username: username,
      password: password
    };
  }

  // Create client
  debug('Connecting...');
  client = new etcd.Etcd3(options);

  // Get cache
  debug('Caching...');
  cache = await all('cns');

  // Create watcher
  debug('Watching...');
  watcher = await watch('cns');

  // Bind handlers
  watcher.on('connected', () => {
    // Re-connect
    debug('Connected...');
  })
  .on('put', async (change) => {
    // Key put
    try {
      const key = change.key.toString();
      const value = change.value.toString();

      await onput(key, value);
    } catch(e) {
      // Failure
      error(e);
    }
  })
  .on('delete', async (change) => {
    // Key deleted
    try {
      const key = change.key.toString();
      const value = cache[key];

      await ondelete(key, value);
    } catch(e) {
      // Failure
      error(e);
    }
  })
  .on('disconnected', () => {
    // Broken connection
    debug('Disconnected...');
  })
  .on('error', (e) => {
    // Failure
    throw new Error(E_WATCH + ': ' + e.message);
  });

  // Success
  print('Network on ' + (username?(username + '@'):'') + host);
}

// cns/{network}/nodes/{node}/contexts/{context}/{role}/{profile}/connections/{connection}/properties

// Key has changed
async function onput(key, value) {
//  debug('PUT ' + key + ' = ' + value);

  // Split key
  const parts = key.split('/');

  const root = parts[0];
  const network = parts[1];
  const property = parts[2];
  const role = parts[6];
  const capability = parts[8];
  const connection = parts[10];

  // Outside scope?
  if (root !== 'cns' || network === undefined) return;

  // Update cache
  cache[key] = value;

  // What network property?
  switch (property) {
    case 'orchestrator':
      // Orchestrator changed
// remove all connections?
      rebuild();
      break;
    case 'nodes':
      // Node changed
      switch (role) {
        case 'provider':
        case 'consumer':
          // Capability changed
          switch (capability) {
            case 'version':
            case 'scope':
              // Version or scope
// remove profile connections?
              rebuild();
              break;
            case 'connections':
              // Connections
              switch (connection) {
                case 'properties':
                  // Connection properties
                  await update(key, value);
                  break;
              }
              break;
            case 'properties':
              // Capability properties
              await propagate(key, value);
              break;
          }
          break;
      }
      break;
    case 'profiles':
      // Profile changed
      rebuild();
      break;
  }
}

// Key is deleted
async function ondelete(key, value) {
//  debug('DEL ' + key, value);

  // Split key
  const parts = key.split('/');

  const root = parts[0];
  const network = parts[1];
  const property = parts[2];
  const role = parts[6];
  const profile = parts[7];
  const capability = parts[8];
  const connection = parts[9];
  const other = parts[10];

  // Outside scope?
  if (root !== 'cns' || network === undefined) return;

  // Update cache
  delete cache[key];

// profile delete
// capability delete

  // What network property?
  switch (property) {
    case 'orchestrator':
      // Orchestrator deleted
// remove all connections?
      break;
    case 'nodes':
      switch (role) {
        case 'provider':
        case 'consumer':
          // Capability
          switch (capability) {
            case 'version':
              // Capability deleted
//            case 'scope':
// remove connections (both ends)
//              rebuild();
              break;
            case 'connections':
              // Other role
              switch (other) {
                case 'provider':
                case 'consumer':
                  // Role deleted
// remove opposite connection
//                  const key2 = value + '/' + role + '/' + profile + '/connections/' + connection;
//                  console.log('WANTS TO PURGE', key2);
                  break;
              }
              // Connection deleted
//              rebuild();
              break;
// delete property? put it back
          }
          break;
      }
      break;
    case 'profiles':
      // Profile deleted
// remove connections using profile
      break;
  }
}

// Update connection property
async function update(key, value) {
  // Split key
  const parts = key.split('/');

  const network = parts[1];
  const node = parts[3];
  const context = parts[5];
  const role = parts[6];
  const profile = parts[7];
  const connection = parts[9];
  const property = parts[11];

  var ns = 'cns/' + network;

  // Get orchestrator mode
  const mode = cache[ns + '/orchestrator'];
  if (!isValidMode(mode)) return;

  ns += '/nodes/' + node + '/contexts/' + context + '/' + role + '/' + profile;

  // Get capability version
  const version = cache[ns + '/version'];
  if (version === null) return;

  // Get property provider
  const provider = cache['cns/' + network + '/profiles/' + profile + '/versions/version' + version + '/properties/' + property + '/provider'];
  if (provider === null) return;

  // Get opposite role
  const opposite = getOppositeRole(role, provider);
  if (opposite === null) return;

  // Find other end
  const other = cache[ns + '/connections/' + connection + '/' + opposite];
  if (other === null) return;

  debug('Updating...');
  debug('  ' + opposite + ' ' + connection + ' ' + property);

  // Set property at other end
  await put(other + '/' + opposite + '/' + profile + '/connections/' + connection + '/properties/' + property, value);
}

// Propagate capability property
async function propagate(key, value) {
  // Split key
  const parts = key.split('/');

  const network = parts[1];
  const node = parts[3];
  const context = parts[5];
  const role = parts[6];
  const profile = parts[7];
  const property = parts[9];

  var ns = 'cns/' + network;

  // Get orchestrator mode
  const mode = cache[ns + '/orchestrator'];
  if (!isValidMode(mode)) return;

  ns += '/nodes/' + node + '/contexts/' + context + '/' + role + '/' + profile;

  // Get capability version
  const version = cache[ns + '/version'];
  if (version === null) return;

  // Get property provider
  const provider = cache['cns/' + network + '/profiles/' + profile + '/versions/version' + version + '/properties/' + property + '/provider'];
  if (provider === null) return;

  // Get opposite role
  const opposite = getOppositeRole(role, provider);
  if (opposite === null) return;

  debug('Propagating...');

  // Update connections
  const connections = filter(cache, ns + '/connections/*/' + opposite);

  for (const key in connections) {
    // Split key
    const parts = key.split('/');
    const connection = parts[9];

    debug('  ' + role + ' ' + connection + ' ' + property);

    parts.pop();
    parts.push('properties');
    parts.push(property);

    // Set connection property
    await put(parts.join('/'), value);
  }
}

// Schedule a rebuid
function rebuild() {
  // Cancel previous
  cancel();

  // Set timer
  timer = setTimeout(async () => {
    // Timer up
    timer = undefined;

    try {
      // Build connections
      await build();
    } catch(e) {
      // Failure
      error(e);
    }
  }, 1000);
}

// Cancel rebuild
function cancel() {
  // Cancel timer?
  if (timer !== undefined) {
    clearTimeout(timer);
    timer = undefined;
  }
}

// Build connections
async function build() {
  debug('Building...');

  const add = [];

  // Look at networks
  const networks = filter(cache, 'cns/*/name');

  for (const key in networks) {
    const parts = key.split('/');
    const network = parts[1];

    const ns1 = 'cns/' + network;

    // Get orchestrator mode
    const mode = cache[ns1 + '/orchestrator'];
    if (!isValidMode(mode)) return;

    debug('Network ' + network);

    // Look at nodes
    const nodes = filter(cache, ns1 + '/nodes/*/name');

    for (const key in nodes) {
      const parts = key.split('/');
      const node = parts[3];

      debug('  Node ' + node);

      // Look at contexts
      const ns2 = ns1 + '/nodes/' + node;
      const contexts = filter(cache, ns2 + '/contexts/*/name');

      for (const key in contexts) {
        const parts = key.split('/');
        const context = parts[5];

        debug('    Context ' + context);

        // Look at providers
        const ns3 = ns2 + '/contexts/' + context;
        const provider = filter(cache, ns3 + '/provider/*/version');

        for (const key in provider) {
          const parts = key.split('/');

          const profile = parts[7];
          const version = provider[key];

          debug('      Provides ' + profile + ' v' + version);

          // Add consumers for provider
          consumers(mode, network, node, context, profile, version, add);
        }
      }
    }
  }

  // Generate new connections
  await connections(add);
}

// Add consumers for provider
function consumers(mode, network, node, context, profile, version, add) {
  // Provider context
  const provider = 'cns/' + network + '/nodes/' + node + '/contexts/' + context;
  const scope = context;

  // What mode?
  switch (mode) {
    case 'allsystems': allsystems(provider, profile, version, scope, add); break;
    case 'bysystem': bysystem(network, provider, profile, version, scope, add); break;
  }
}

// Add network consumers
function allsystems(provider, profile, version, scope, add) {
  // Look through networks
  const networks = filter(cache, 'cns/*/name');

  for (const key in networks) {
    const parts = key.split('/');
    const network = parts[1];

    // Add node consumers
    bysystem(network, provider, profile, version, scope, add);
  }
}

// Add node consumers
function bysystem(network, provider, profile, version, scope, add) {
  // Look through nodes
  const nodes = filter(cache, 'cns/' + network + '/nodes/*/name');

  for (const key in nodes) {
    const parts = key.split('/');
    const node = parts[3];

    // Look through contexts
    const contexts = filter(cache, 'cns/' + network + '/nodes/' + node + '/contexts/*/name');

    for (const key in contexts) {
      const parts = key.split('/');
      const context = parts[5];

      // Context must match
      if (context === scope) {
        // Look through capabilities
        const consumer = 'cns/' + network + '/nodes/' + node + '/contexts/' + context;
        const capabilities = filter(cache, consumer + '/consumer/' + profile + '/version');

        for (const key in capabilities) {
          // Same profile version?
          if (capabilities[key] === version) {
            // Add possible connection
            add.push({
              provider: provider,
              consumer: consumer,
              profile: profile,
              version: version
            });
          }
        }
      }
    }
  }
}

// Add missing connections
async function connections(add) {
  debug('Connecting...');

  // Look through possible connections
  for (const c of add) {
    // Look for existing connection
    const provider = filter(cache, c.provider + '/provider/' + c.profile + '/connections/*/consumer');
    const consumer = filter(cache, c.consumer + '/consumer/' + c.profile + '/connections/*/provider');

    var id = null;

    var addp = true;
    var addc = true;

    for (const key in provider) {
      if (provider[key] === c.consumer) {
        // Provider connection exists
        id = key.split('/')[9];
        addp = false;
        break;
      }
    }

    for (const key in consumer) {
      if (consumer[key] === c.provider) {
        // Consumer connection exists
        id = key.split('/')[9];
        addc = false;
        break;
      }
    }

    // Connection already exists?
    if (!addp && !addc) {
      debug('  Existing ' + id);
      continue;
    }

    // Merge connection defaults
    const properties = {};

    const propsp = filter(cache, c.provider + '/provider/' + c.profile + '/properties/*');
    const propsc = filter(cache, c.consumer + '/consumer/' + c.profile + '/properties/*');

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

    // Needs new id?
    if (id === null) id = short.generate();

    debug('  Creating ' + id);

    // Add provider connection?
    if (addp) {
      const ns = c.provider + '/provider/' + c.profile + '/connections/' + id + '/';
      await put(ns + 'consumer', c.consumer);

      for (const name in properties)
        await put(ns + 'properties/' + name, properties[name]);
    }

    // Add consumer connection?
    if (addc) {
      const ns = c.consumer + '/consumer/' + c.profile + '/connections/' + id + '/';
      await put(ns + 'provider', c.provider);

      for (const name in properties)
        await put(ns + 'properties/' + name, properties[name]);
    }
  }
}

// Is valid mode
function isValidMode(mode) {
  // What mode?
  switch (mode) {
    case 'allsystems':
    case 'bysystem':
      return true;
  }
  return false;
}

// Get opposite role
function getOppositeRole(role, provider) {
  switch (role) {
    case 'provider': if (provider === 'yes') return 'consumer'; break;
    case 'consumer': if (provider !== 'yes') return 'provider'; break;
  }
  return null;
}

// Watch prefix
async function watch(prefix) {
  // Must be connected
  if (client === undefined)
    throw new Error(E_CONNECT);

  return await client.watch()
    .prefix(prefix)
    .create()
    .catch((e) => {
      // Failure
      throw new Error(E_WATCH + ': ' + e.message);
    });
}

// Get all keys
async function all(prefix) {
  // Must be connected
  if (client === undefined)
    throw new Error(E_CONNECT);

  return await client.getAll()
    .prefix(prefix)
    .strings()
    .catch((e) => {
      // Failure
      throw new Error(E_ALL + ': ' + e.message);
    });
}

// Get key value
async function get(key) {
  // Must be connected
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
  // Must be connected
  if (client === undefined)
    throw new Error(E_CONNECT);

  return await client.put(key)
    .value(value)
    .catch((e) => {
      // Failure
      throw new Error(E_PUT + ': ' + e.message);
    });
}

// Filter keys
function filter(keys, filter) {
  const result = {};
  const filters = filter.split('/');

  for (const key in keys) {
    if (compare(key, filters))
      result[key] = keys[key];
  }
  return result;
}

// Compare key with filters
function compare(key, filters) {
  const keys = key.split('/');

  if (keys.length === filters.length) {
    for (var n = 0; n < keys.length; n++)
      if (!match(keys[n], filters[n])) return false;

    return true;
  }
  return false;
}

// Wilcard match
function match(text, filter) {
  const esc = (s) => s.replace(/([.*+?^=!:${}()|\[\]\/\\])/g, '\\$1');
  return new RegExp('^' + filter.split('*').map(esc).join('.*') + '$', 'i').test(text);
}

// Disconnect client
async function disconnect() {
  // Close watcher?
  if (watcher !== undefined) {
    debug('Unwatching...');

    await watcher.cancel();
    watcher = undefined;
  }

  // Close client?
  if (client !== undefined) {
    debug('Disconnecting...');

    await client.close();
    client = undefined;
  }
}

// Terminate application
async function exit(code) {
  cancel();

  if (client !== undefined)
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
