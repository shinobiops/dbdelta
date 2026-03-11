#!/usr/bin/env node

import { connect } from './connection.js';
import { introspectDb } from './introspector/index.js';
import { buildDependencyInfo } from './dependencies.js';
import { diff } from './differ.js';
import { generate } from './sql-generator.js';

const SYSTEM_SCHEMAS = ['pg_catalog', 'information_schema', 'pg_toast'];

export function parseArgs(argv) {
  const args = argv.slice(2);
  const result = {
    fromUrl: null,
    toUrl: null,
    schemas: ['public'],
    excludeSchemas: [],
    excludeTypes: [],
    renames: [],
  };

  const positional = [];
  let i = 0;
  while (i < args.length) {
    const arg = args[i];
    if (arg === '--schemas' && i + 1 < args.length) {
      result.schemas = args[++i].split(',').map(s => s.trim());
    } else if (arg === '--exclude-schemas' && i + 1 < args.length) {
      result.excludeSchemas = [
        ...SYSTEM_SCHEMAS,
        ...args[++i].split(',').map(s => s.trim()).filter(Boolean),
      ];
    } else if (arg === '--exclude-types' && i + 1 < args.length) {
      result.excludeTypes = args[++i].split(',').map(s => s.trim());
    } else if (arg === '--rename' && i + 1 < args.length) {
      result.renames.push(args[++i]);
    } else if (!arg.startsWith('--')) {
      positional.push(arg);
    } else {
      throw new Error(`Unknown option: ${arg}`);
    }
    i++;
  }

  if (positional.length < 2) {
    throw new Error('Usage: dbdelta <fromUrl> <toUrl> [options]');
  }
  result.fromUrl = positional[0];
  result.toUrl = positional[1];

  // Always exclude system schemas
  if (result.excludeSchemas.length === 0) {
    result.excludeSchemas = [...SYSTEM_SCHEMAS];
  }

  return result;
}

export function parseRename(spec) {
  const parts = spec.split(':');
  if (parts.length !== 3) {
    throw new Error(`Invalid rename spec: ${spec}`);
  }
  const [kind, from, to] = parts;
  if (kind === 'table') {
    return { kind: 'table', from, to };
  }
  if (kind === 'column') {
    const slashIdx = from.indexOf('/');
    if (slashIdx === -1) {
      throw new Error(`Column rename must use / to separate table from column: ${spec}`);
    }
    const tablePart = from.slice(0, slashIdx);
    const column = from.slice(slashIdx + 1);
    let schema = 'public';
    let table = tablePart;
    const dotIdx = tablePart.indexOf('.');
    if (dotIdx !== -1) {
      schema = tablePart.slice(0, dotIdx);
      table = tablePart.slice(dotIdx + 1);
    }
    return { kind: 'column', schema, table, from: column, to };
  }
  throw new Error(`Unknown rename kind: ${kind}`);
}

function progress(msg) {
  process.stderr.write(`[dbdelta] ${msg}\n`);
}

async function main() {
  let fromClient;
  let toClient;

  try {
    const config = parseArgs(process.argv);

    progress('connecting to databases...');
    fromClient = await connect(config.fromUrl);
    toClient = await connect(config.toUrl);

    progress('introspecting schemas...');
    const [fromObjects, toObjects] = await Promise.all([
      introspectDb(fromClient, config.schemas),
      introspectDb(toClient, config.schemas),
    ]);
    progress(`found ${fromObjects.length} objects in source, ${toObjects.length} in target`);

    progress('building dependency graph...');
    const depInfo = await buildDependencyInfo(fromClient, toClient, config.schemas);

    progress('computing differences...');
    const operations = diff(fromObjects, toObjects, {
      renames: config.renames,
      excludeTypes: config.excludeTypes,
      excludeSchemas: config.excludeSchemas,
    });
    progress(`found ${operations.length} change operations`);

    progress('generating SQL...');
    const sql = generate(operations, depInfo);

    process.stdout.write(sql);
    progress('done.');
  } catch (err) {
    process.stderr.write(`[dbdelta] error: ${err.message}\n`);
    process.exit(1);
  } finally {
    await Promise.allSettled([
      fromClient && fromClient.end(),
      toClient && toClient.end(),
    ]);
  }
}

const isMain = process.argv[1] && import.meta.url.endsWith(process.argv[1].replace(/\\/g, '/'));
if (isMain) {
  main();
}
