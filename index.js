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
const E_BUILD = 'Failed to build';
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
var mode;
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

  // Get orchestrator mode
  mode = await get('cns/network/orchestrator');

  // Create watcher
  debug('Watching...');
  watcher = (await watch('cns/network'))
    .on('connected', () => {
      // Re-connect
      debug('Connected...');
    })
    .on('put', async (change) => {
      // Key put
      await onput(change);
    })
    .on('delete', async (change) => {
      // Key deleted
      await ondelete(change);
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
  print('Network on ' + host + (username?(' as ' + username):''));
}

// Put event handler
async function onput(change) {
  try {
    // Get change
    const key = change.key.toString();
    const value = change.value.toString();

//    debug('PUT ' + key + ' = ' + value);

    // Creating or modifying key?
    if (change.version === '1')
      await created(key, value);
    else await modified(key, value);
  } catch(e) {
    // Failure
    error(e);
  }
}

// Delete event handler
async function ondelete(change) {
  try {
    // Get change
    const key = change.key.toString();
    const value = change.value.toString();

//    debug('DEL ' + key);

    // Deleting key
    await deleted(key, value);
  } catch(e) {
    // Failure
    error(e);
  }
}

// Is valid mode
function isValidMode() {
  // What mode?
  switch (mode) {
    case 'nodes':
    case 'contexts':
      return true;
  }
  return false;
}

// cns/network/nodes/{node}/contexts/{context}/{role}/{profile}/connections/{connection}/properties

// Key is created
async function created(key, value) {
  const parts = key.split('/');

  const network = parts[2];
  const role = parts[6];
  const capability = parts[8];

  switch (network) {
    case 'orchestrator':
      // Orchestrator created
      mode = value;
      rebuild();
      break;
    case 'nodes':
      // Node created
      switch (role) {
        case 'provider':
        case 'consumer':
          // Capability created
          switch (capability) {
            case 'version':
              // Version created
              rebuild();
              break;
          }
          break;
      }
      break;
    case 'profiles':
      // Profile created
      rebuild();
      break;
  }
}

// Key has changed
async function modified(key, value) {
  const parts = key.split('/');

  const network = parts[2];
  const role = parts[6];
  const capability = parts[8];
  const connection = parts[10];

  switch (network) {
    case 'orchestrator':
      // Orchestrator changed
      mode = value;
// remove all connections
//      rebuild();
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
      break;
  }
}

// Key is deleted
async function deleted(key, value) {
  const parts = key.split('/');

  const network = parts[2];
  const role = parts[6];
  const capability = parts[8];

// profile delete
// capability delete

  switch (network) {
    case 'orchestrator':
      // Orchestrator deleted
      mode = 'none';
// remove all connections
      break;
    case 'nodes':
      switch (role) {
        case 'provider':
        case 'consumer':
          // Capability
          switch (capability) {
            case 'version':
            case 'scope':
    // remove connections
              rebuild();
              break;
            case 'connections':
              // Connection deleted
              rebuild();
              break;
    // delete property? put it back
          }
          break;
      }
      break;
    case 'profiles':
      // Profile deleted
      break;
  }
}

// Update connection property
async function update(key, value) {
  // Mode check
  if (!isValidMode()) return;

  // Split key
  const parts = key.split('/');

  const node = parts[3];
  const context = parts[5];
  const role = parts[6];
  const profile = parts[7];
  const connection = parts[9];
  const property = parts[11];

  // Get capability version
  const version = await get('cns/network/nodes/' + node + '/contexts/' + context + '/' + role + '/' + profile + '/version');
  if (version === null) return;

  // Get property provider
  const provider = await get('cns/network/profiles/' + profile + '/versions/version' + version + '/properties/' + property + '/provider');
  if (provider === null) return;

  var anti;

  switch (role) {
    case 'provider':
      if (provider === 'yes') anti = 'consumer';
      break;
    case 'consumer':
      if (provider !== 'yes') anti = 'provider';
      break;
  }

  if (anti === undefined) return;

// mode === contexts check nodeA === nodeB

  // Find other end
  const other = await get('cns/network/nodes/' + node + '/contexts/' + context + '/' + role + '/' + profile + '/connections/' + connection + '/' + anti);
  if (other === null) return;

  debug('Updating ' + anti + ' ' + connection + ' ' + property);

  // Set property at other end
  await put(other + '/' + anti + '/' + profile + '/connections/' + connection + '/properties/' + property, value);
}

//
async function propagate(key, value) {
  // Mode check
  if (!isValidMode()) return;

  // Split key
  const parts = key.split('/');

// cns/network/nodes/{node}/contexts/{context}/{role}/{rpofile}/properties/{property}

  const node = parts[3];
  const context = parts[5];
  const role = parts[6];
  const profile = parts[7];
  const property = parts[9];

  // Get capability version
  const version = await get('cns/network/nodes/' + node + '/contexts/' + context + '/' + role + '/' + profile + '/version');
  if (version === null) return;

  // Get property provider
  const provider = await get('cns/network/profiles/' + profile + '/versions/version' + version + '/properties/' + property + '/provider');
  if (provider === null) return;

  var anti;

  switch (role) {
    case 'provider':
      if (provider === 'yes') anti = 'consumer';
      break;
    case 'consumer':
      if (provider !== 'yes') anti = 'provider';
      break;
  }

  if (anti === undefined) return;

  // Read network cache
  const network = await all('cns/network/nodes/' + node + '/contexts/' + context + '/' + role + '/' + profile + '/connections');
  const connections = filter(network, 'cns/network/nodes/' + node + '/contexts/' + context + '/' + role + '/' + profile + '/connections/*/' + anti);

  for (const key in connections) {
    const parts = key.split('/');
    const connection = parts[9];

    debug('Propagate ' + role + ' ' + connection + ' ' + property);

    // Set connection property
    await put('cns/network/nodes/' + node + '/contexts/' + context + '/' + role + '/' + profile + '/connections/' + connection + '/properties/' + property, value);
  }
}

// Schedule a rebuid
function rebuild() {
  // Cancel previous
  cancel();

  // Mode check
  if (!isValidMode()) return;

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
  // Mode check
  if (!isValidMode()) return;

  debug('Building...');

  // Read network cache
  const network = await all('cns/network');

  const add = [];
  const nodes = filter(network, 'cns/network/nodes/*/name');

  // Look at nodes
  for (const key in nodes) {
    const parts = key.split('/');
    const node = parts[3];

    debug('  Node ' + node);

    const contexts = filter(network, 'cns/network/nodes/' + node + '/contexts/*/name');

    // Look at contexts
    for (const key in contexts) {
      const parts = key.split('/');
      const context = parts[5];

      debug('    Context ' + context);

      const provider = filter(network, 'cns/network/nodes/' + node + '/contexts/' + context + '/provider/*/version');
      const consumer = filter(network, 'cns/network/nodes/' + node + '/contexts/' + context + '/consumer/*/version');

      // Look at providers
      for (const key in provider) {
        const parts = key.split('/');
        const profile = parts[7];
        const version = provider[key];

        debug('      Provides ' + profile + ' v' + version);

        // Add consumers for provider
        consumers(network, node, context, profile, version, add);
      }

      // Look at consumers
      for (const key in consumer) {
        const parts = key.split('/');
        const profile = parts[7];
        const version = consumer[key];

        debug('      Consumes ' + profile + ' v' + version);
      }
    }
  }

  // Generate new connections
  await connections(network, add);
}

// Add consumers for provider
function consumers(network, node, context, profile, version, add) {
  const provider = 'cns/network/nodes/' + node + '/contexts/' + context;

  // What mode?
  switch (mode) {
    case 'nodes': nodes(network, provider, profile, version, add); break;
    case 'contexts': contexts(network, provider, node, profile, version, add); break;
  }
}

// Add node consumers
function nodes(network, provider, profile, version, add) {
  const nodes = filter(network, 'cns/network/nodes/*/name');

  // Look through nodes
  for (const key in nodes) {
    const parts = key.split('/');
    const node = parts[3];

    // Add context consumers
    contexts(network, provider, node, profile, version, add);
  }
}

// Add context consumers
function contexts(network, provider, node, profile, version, add) {
  const contexts = filter(network, 'cns/network/nodes/' + node + '/contexts/*/name');

  // Look through contexts
  for (const key in contexts) {
    const parts = key.split('/');
    const context = parts[5];

    // Add capability consumers
    capabilities(network, provider, node, context, profile, version, add);
  }
}

// Add capability consumers
function capabilities(network, provider, node, context, profile, version, add) {
  const consumer = 'cns/network/nodes/' + node + '/contexts/' + context;
  const capabilities = filter(network, consumer + '/consumer/' + profile + '/version');

  // Look through capabilities
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

// Add missing connections
async function connections(network, add) {
  // Look through possible connections
  for (const c of add) {
    // Look for existing connection
    const provider = filter(network, c.provider + '/provider/' + c.profile + '/connections/*/consumer');
    const consumer = filter(network, c.consumer + '/consumer/' + c.profile + '/connections/*/provider');

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

// check properties exist

      debug('Connection exists ' + id);
      continue;
    }

    // Merge connection defaults
    const properties = {};

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

// switch (idformat)

    // Needs new id?
    if (id === null) id = short.generate();

    debug('Creating connection ' + id);

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
