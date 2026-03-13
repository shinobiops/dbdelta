#!/usr/bin/env node

import { connect } from './connection.js';
import { introspectDb } from './introspector/index.js';
import { buildDependencyInfo } from './dependencies.js';
import { diff } from './differ.js';
import { generate } from './sql-generator.js';

const SYSTEM_SCHEMAS = ['pg_catalog', 'information_schema', 'pg_toast'];

const HELP_TEXT = `\
dbdelta - compare two PostgreSQL databases and generate migration SQL

Usage:
  dbdelta <fromUrl> <toUrl> [options]

Arguments:
  <fromUrl>   PostgreSQL connection string for the source (current) database
  <toUrl>     PostgreSQL connection string for the target (desired) database

  Connection strings use the standard PostgreSQL format:
    postgresql://user:password@host:port/dbname

  The tool compares source → target and generates SQL that, when applied to
  the source database, transforms its schema to match the target.

Options:
  -h, --help                  Show this help message and exit

  --schemas <list>            Comma-separated schemas to compare
                              (default: public)
                              Example: --schemas public,app,auth

  --exclude-schemas <list>    Comma-separated schemas to skip (in addition to
                              system schemas which are always excluded:
                              pg_catalog, information_schema, pg_toast)
                              Example: --exclude-schemas audit,logs

  --exclude-types <list>      Comma-separated object types to skip entirely
                              Example: --exclude-types grants,roles
                              See "Object types" below for valid values.

  --rename <spec>             Declare a rename so the differ matches objects by
                              identity instead of treating them as drop + create.
                              Can be specified multiple times.

    Table rename format:      --rename table:old_name:new_name
                              --rename table:schema.old_name:new_name
                              Schema defaults to "public" if omitted.

    Column rename format:     --rename column:table/old_col:new_col
                              --rename column:schema.table/old_col:new_col
                              Uses "/" to separate table from column name.
                              Schema defaults to "public" if omitted.

Object types:
  The following object types are introspected and diffed. Use these names
  with --exclude-types.

  Schema-level:       schema, extension, sequence
  Types:              enum, composite_type, domain, range
  Tables:             table, column, constraint, index
  Code:               function, procedure, view, materialized_view
  Triggers & rules:   trigger, rule, policy, event_trigger
  Operators:          operator, opclass, aggregate, cast
  Text search:        ts_config, ts_dictionary, ts_parser, ts_template
  Statistics:         statistics, collation
  Foreign data:       foreign_data_wrapper, foreign_server, foreign_table,
                      user_mapping
  Replication:        publication, subscription
  Access control:     role, grant

Output:
  SQL is written to stdout. Progress and diagnostics go to stderr.
  Pipe stdout to a file to capture the migration script:

    dbdelta postgres://localhost/mydb postgres://localhost/mydb_new > migrate.sql

  The generated SQL is organized in three phases:
    1. Drops       (commented out for manual review)
    2. Creates & alters  (dependency-ordered)
    3. Grants & revokes  (alphabetical)

Examples:
  # Basic comparison of two databases
  dbdelta postgres://localhost/prod postgres://localhost/staging

  # Compare only specific schemas
  dbdelta postgres://localhost/prod postgres://localhost/staging \\
    --schemas public,app

  # Skip roles and grants
  dbdelta postgres://localhost/prod postgres://localhost/staging \\
    --exclude-types roles,grants

  # Handle a renamed table and column
  dbdelta postgres://localhost/prod postgres://localhost/staging \\
    --rename table:users:accounts \\
    --rename column:accounts/email_address:email
`;

export function parseArgs(argv) {
  const args = argv.slice(2);

  if (args.includes('--help') || args.includes('-h')) {
    process.stdout.write(HELP_TEXT);
    process.exit(0);
  }

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
    throw new Error('Usage: dbdelta <fromUrl> <toUrl> [options]\nRun "dbdelta --help" for full usage information.');
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
      schemas: config.schemas,
    });
    progress(`found ${operations.length} change operations`);

    progress('generating SQL...');
    const sql = generate(operations, depInfo, fromObjects);

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
