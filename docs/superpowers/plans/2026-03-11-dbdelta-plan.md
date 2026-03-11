# dbdelta Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Node.js CLI that compares two PostgreSQL databases and generates ordered migration SQL to stdout.

**Architecture:** Query system catalogs from both DBs in parallel, build dependency graph from pg_depend, diff matched objects, emit topologically-sorted SQL. Single dependency: pg.

**Tech Stack:** Node.js (ESM), pg (node-postgres), node:test

**Spec:** `docs/superpowers/specs/2026-03-11-dbdelta-design.md`

---

## Chunk 1: Project Setup, CLI, Connection, Introspection Framework

This chunk establishes the project scaffolding: package.json, CLI arg parsing, database connection helpers, test infrastructure, identity helpers, and two reference introspectors (schemas + extensions) plus the orchestrator that runs all introspectors in parallel.

### Task 1.1 — Initialize the project

- [ ] Create `package.json` with ESM module type and pg dependency
- [ ] Run `npm install` and create directory structure

**File: `package.json`**

```json
{
  "name": "dbdelta",
  "version": "0.1.0",
  "description": "Compare two PostgreSQL databases and generate migration SQL",
  "type": "module",
  "bin": {
    "dbdelta": "./src/cli.js"
  },
  "scripts": {
    "test": "node --test test/**/*.test.js"
  },
  "dependencies": {
    "pg": "^8.13.0"
  }
}
```

**Commands:**

```bash
cd /Users/venatir/work/personal/dbdelta
npm install
mkdir -p src/introspector test/helpers
```

- [ ] **Commit:** `git add package.json package-lock.json && git commit -m "chore: initialize project with pg dependency"`

---

### Task 1.2 — CLI argument parser

- [ ] Create `src/cli.js` with argument parsing
- [ ] Write tests for CLI arg parsing
- [ ] Run tests, verify pass
- [ ] Commit

**File: `src/cli.js`**

```js
#!/usr/bin/env node

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
      result.excludeSchemas = args[++i].split(',').map(s => s.trim());
    } else if (arg === '--exclude-types' && i + 1 < args.length) {
      result.excludeTypes = args[++i].split(',').map(s => s.trim());
    } else if (arg === '--rename' && i + 1 < args.length) {
      result.renames.push(parseRename(args[++i]));
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

async function main() {
  try {
    const config = parseArgs(process.argv);
    console.error('dbdelta: parsed config', JSON.stringify(config, null, 2));
  } catch (err) {
    console.error(`Error: ${err.message}`);
    process.exit(1);
  }
}

const isMain = process.argv[1] && import.meta.url.endsWith(process.argv[1].replace(/\\/g, '/'));
if (isMain) {
  main();
}
```

**File: `test/cli.test.js`**

```js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { parseArgs, parseRename } from '../src/cli.js';

describe('parseArgs', () => {
  it('parses two positional urls with defaults', () => {
    const result = parseArgs(['node', 'cli.js', 'postgres://localhost/from', 'postgres://localhost/to']);
    assert.equal(result.fromUrl, 'postgres://localhost/from');
    assert.equal(result.toUrl, 'postgres://localhost/to');
    assert.deepEqual(result.schemas, ['public']);
  });

  it('parses --schemas flag', () => {
    const result = parseArgs(['node', 'cli.js', 'from', 'to', '--schemas', 'public,app']);
    assert.deepEqual(result.schemas, ['public', 'app']);
  });

  it('parses --exclude-schemas flag', () => {
    const result = parseArgs(['node', 'cli.js', 'from', 'to', '--exclude-schemas', 'audit,logs']);
    assert.deepEqual(result.excludeSchemas, ['audit', 'logs']);
  });

  it('parses --exclude-types flag', () => {
    const result = parseArgs(['node', 'cli.js', 'from', 'to', '--exclude-types', 'grants,roles']);
    assert.deepEqual(result.excludeTypes, ['grants', 'roles']);
  });

  it('parses multiple --rename flags', () => {
    const result = parseArgs([
      'node', 'cli.js', 'from', 'to',
      '--rename', 'table:users:accounts',
      '--rename', 'column:app.orders/qty:quantity',
    ]);
    assert.equal(result.renames.length, 2);
    assert.deepEqual(result.renames[0], { kind: 'table', from: 'users', to: 'accounts' });
    assert.deepEqual(result.renames[1], { kind: 'column', schema: 'app', table: 'orders', from: 'qty', to: 'quantity' });
  });

  it('throws on missing positional args', () => {
    assert.throws(() => parseArgs(['node', 'cli.js']), /Usage/);
  });

  it('throws on unknown option', () => {
    assert.throws(() => parseArgs(['node', 'cli.js', 'from', 'to', '--bogus']), /Unknown option/);
  });
});

describe('parseRename', () => {
  it('parses table rename', () => {
    assert.deepEqual(parseRename('table:old:new'), { kind: 'table', from: 'old', to: 'new' });
  });

  it('parses column rename with schema', () => {
    assert.deepEqual(parseRename('column:myschema.mytable/old_col:new_col'), {
      kind: 'column', schema: 'myschema', table: 'mytable', from: 'old_col', to: 'new_col',
    });
  });

  it('parses column rename defaulting to public schema', () => {
    assert.deepEqual(parseRename('column:mytable/old_col:new_col'), {
      kind: 'column', schema: 'public', table: 'mytable', from: 'old_col', to: 'new_col',
    });
  });

  it('throws on column rename without /', () => {
    assert.throws(() => parseRename('column:table.old_col:new_col'), /must use \//);
  });

  it('throws on unknown rename kind', () => {
    assert.throws(() => parseRename('index:old:new'), /Unknown rename kind/);
  });
});
```

**Run:** `node --test test/cli.test.js`

- [ ] **Commit:** `git add src/cli.js test/cli.test.js && git commit -m "feat: add CLI argument parser with rename support"`

---

### Task 1.3 — Database connection helper + test infrastructure

- [ ] Create `src/connection.js`
- [ ] Create `test/helpers/db.js` for test database setup/teardown
- [ ] Write connection test
- [ ] Commit

**File: `src/connection.js`**

```js
import pg from 'pg';
const { Client } = pg;

export async function connect(connectionString) {
  const client = new Client({ connectionString });
  await client.connect();
  return client;
}
```

**File: `test/helpers/db.js`**

```js
import pg from 'pg';
const { Client } = pg;

const ADMIN_URL = process.env.DBDELTA_TEST_ADMIN_URL || 'postgres://localhost/postgres';
const FROM_DB = 'dbdelta_test_from';
const TO_DB = 'dbdelta_test_to';

export function getTestUrls() {
  const base = ADMIN_URL.replace(/\/[^/]*$/, '');
  return {
    fromUrl: `${base}/${FROM_DB}`,
    toUrl: `${base}/${TO_DB}`,
  };
}

export async function setupTestDatabases() {
  const admin = new Client({ connectionString: ADMIN_URL });
  await admin.connect();
  try {
    for (const db of [FROM_DB, TO_DB]) {
      await admin.query(`
        select pg_terminate_backend(pid)
        from pg_stat_activity
        where datname = '${db}' and pid <> pg_backend_pid()
      `);
      await admin.query(`drop database if exists ${db}`);
      await admin.query(`create database ${db}`);
    }
  } finally {
    await admin.end();
  }
  return getTestUrls();
}

export async function teardownTestDatabases() {
  const admin = new Client({ connectionString: ADMIN_URL });
  await admin.connect();
  try {
    for (const db of [FROM_DB, TO_DB]) {
      await admin.query(`
        select pg_terminate_backend(pid)
        from pg_stat_activity
        where datname = '${db}' and pid <> pg_backend_pid()
      `);
      await admin.query(`drop database if exists ${db}`);
    }
  } finally {
    await admin.end();
  }
}

export async function connectTo(url) {
  const client = new Client({ connectionString: url });
  await client.connect();
  return client;
}
```

**File: `test/connection.test.js`**

```js
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { connect } from '../src/connection.js';
import { setupTestDatabases, teardownTestDatabases, getTestUrls } from './helpers/db.js';

describe('connection', () => {
  before(async () => { await setupTestDatabases(); });
  after(async () => { await teardownTestDatabases(); });

  it('connects to a postgres database and runs a query', async () => {
    const { fromUrl } = getTestUrls();
    const client = await connect(fromUrl);
    try {
      const result = await client.query('select 1 as num');
      assert.equal(result.rows[0].num, 1);
    } finally {
      await client.end();
    }
  });
});
```

**Run:** `node --test test/connection.test.js`

- [ ] **Commit:** `git add src/connection.js test/helpers/db.js test/connection.test.js && git commit -m "feat: add database connection helper and test infrastructure"`

---

### Task 1.4 — Identity helpers

- [ ] Create `src/introspector/identity.js`
- [ ] Write tests
- [ ] Commit

**File: `src/introspector/identity.js`**

```js
export function identity(schema, type, name) {
  return { schema, type, name };
}

export function identityKey(id) {
  return `${id.schema}.${id.type}.${id.name}`;
}
```

**File: `test/identity.test.js`**

```js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { identity, identityKey } from '../src/introspector/identity.js';

describe('identity', () => {
  it('creates an identity object', () => {
    const id = identity('public', 'table', 'users');
    assert.deepEqual(id, { schema: 'public', type: 'table', name: 'users' });
  });
});

describe('identityKey', () => {
  it('creates a string key from identity', () => {
    assert.equal(identityKey({ schema: 'public', type: 'table', name: 'users' }), 'public.table.users');
  });

  it('produces different keys for different identities', () => {
    const a = identityKey({ schema: 'public', type: 'table', name: 'users' });
    const b = identityKey({ schema: 'app', type: 'table', name: 'users' });
    assert.notEqual(a, b);
  });
});
```

**Run:** `node --test test/identity.test.js`

- [ ] **Commit:** `git add src/introspector/identity.js test/identity.test.js && git commit -m "feat: add identity helpers for object matching"`

---

### Task 1.5 — Schemas introspector

- [ ] Create `src/introspector/schemas.js`
- [ ] Write tests
- [ ] Commit

See Chunk 1 agent output for full code. Key query: `pg_namespace` filtered by schema list, excluding system schemas.

**Run:** `node --test test/schemas.test.js`

- [ ] **Commit:** `git add src/introspector/schemas.js test/schemas.test.js && git commit -m "feat: add schemas introspector"`

---

### Task 1.6 — Extensions introspector

- [ ] Create `src/introspector/extensions.js`
- [ ] Write tests
- [ ] Commit

See Chunk 1 agent output for full code. Key query: `pg_extension` joined to `pg_namespace`.

**Run:** `node --test test/extensions.test.js`

- [ ] **Commit:** `git add src/introspector/extensions.js test/extensions.test.js && git commit -m "feat: add extensions introspector"`

---

### Task 1.7 — Introspection orchestrator

- [ ] Create `src/introspector/index.js`
- [ ] Write tests
- [ ] Commit

Orchestrator connects to both DBs, runs all introspectors in parallel, returns a combined Map of `identityKey -> { identity, fromDef, toDef, fromDdl, toDdl }`.

**Run:** `node --test test/introspector.test.js`

- [ ] **Commit:** `git add src/introspector/index.js test/introspector.test.js && git commit -m "feat: add introspection orchestrator"`

---

### Task 1.8 — Run all Chunk 1 tests

- [ ] `node --test test/cli.test.js test/identity.test.js test/connection.test.js test/schemas.test.js test/extensions.test.js test/introspector.test.js`

---

## Chunk 2: All Introspector Modules

Each introspector follows the pattern established in Chunk 1: function taking `(client, schemas)` returning `[{identity, definition, ddl}]`. Full code for each module is in the agent output transcripts.

### Task 2.1 — Types introspector (enums, composites, domains, ranges)
- [ ] Create `src/introspector/types.js` — queries `pg_type`, `pg_enum`, `pg_range`, `pg_constraint`
- [ ] Write `test/types.test.js`
- [ ] Commit

### Task 2.2 — Tables introspector
- [ ] Create `src/introspector/tables.js` — queries `pg_class` for `relkind in ('r', 'p')`
- [ ] Write `test/tables.test.js`
- [ ] Commit

### Task 2.3 — Columns introspector
- [ ] Create `src/introspector/columns.js` — queries `pg_attribute` + `pg_attrdef`
- [ ] Write `test/columns.test.js`
- [ ] Commit

### Task 2.4 — Constraints introspector
- [ ] Create `src/introspector/constraints.js` — queries `pg_constraint` for PK, FK, unique, check, exclusion
- [ ] Write `test/constraints.test.js`
- [ ] Commit

### Task 2.5 — Indexes introspector
- [ ] Create `src/introspector/indexes.js` — queries `pg_index` + `pg_class`, uses `pg_get_indexdef`
- [ ] Write `test/indexes.test.js`
- [ ] Commit

### Task 2.6 — Sequences introspector
- [ ] Create `src/introspector/sequences.js` — queries `pg_sequence` + `pg_class` + `pg_depend`
- [ ] Write `test/sequences.test.js`
- [ ] Commit

### Task 2.7 — Functions & Procedures introspector
- [ ] Create `src/introspector/functions.js` — queries `pg_proc`, uses `pg_get_functiondef`
- [ ] Write `test/functions.test.js`
- [ ] Commit

### Task 2.8 — Views introspector
- [ ] Create `src/introspector/views.js` — queries `pg_class` with `relkind = 'v'`, uses `pg_get_viewdef`
- [ ] Write `test/views.test.js`
- [ ] Commit

### Task 2.9 — Materialized Views introspector
- [ ] Create `src/introspector/materialized-views.js` — queries `pg_class` with `relkind = 'm'`
- [ ] Write `test/materialized-views.test.js`
- [ ] Commit

### Task 2.10 — Triggers introspector
- [ ] Create `src/introspector/triggers.js` — queries `pg_trigger`, uses `pg_get_triggerdef`
- [ ] Write `test/triggers.test.js`
- [ ] Commit

### Task 2.11 — Rules introspector
- [ ] Create `src/introspector/rules.js` — queries `pg_rewrite`, uses `pg_get_ruledef`, excludes `_RETURN`
- [ ] Write `test/rules.test.js`
- [ ] Commit

### Task 2.12 — RLS Policies introspector
- [ ] Create `src/introspector/policies.js` — queries `pg_policy`, uses `pg_get_expr`
- [ ] Write `test/policies.test.js`
- [ ] Commit

### Task 2.13 — Operators introspector
- [ ] Create `src/introspector/operators.js` — queries `pg_operator`
- [ ] Write `test/operators.test.js`
- [ ] Commit

### Task 2.14 — Operator Classes & Families introspector
- [ ] Create `src/introspector/opclasses.js` — queries `pg_opclass` + `pg_opfamily`
- [ ] Write `test/opclasses.test.js`
- [ ] Commit

### Task 2.15 — Aggregates introspector
- [ ] Create `src/introspector/aggregates.js` — queries `pg_aggregate` + `pg_proc`
- [ ] Write `test/aggregates.test.js`
- [ ] Commit

### Task 2.16 — Casts introspector
- [ ] Create `src/introspector/casts.js` — queries `pg_cast` + `pg_type`
- [ ] Write `test/casts.test.js`
- [ ] Commit

### Task 2.17 — Collations introspector
- [ ] Create `src/introspector/collations.js` — queries `pg_collation`
- [ ] Write `test/collations.test.js`
- [ ] Commit

### Task 2.18 — Text Search introspector
- [ ] Create `src/introspector/text-search.js` — queries `pg_ts_config`, `pg_ts_dict`, `pg_ts_parser`, `pg_ts_template`
- [ ] Write `test/text-search.test.js`
- [ ] Commit

### Task 2.19 — Statistics introspector
- [ ] Create `src/introspector/statistics.js` — queries `pg_statistic_ext`
- [ ] Write `test/statistics.test.js`
- [ ] Commit

### Task 2.20 — FDW, Foreign Servers, Foreign Tables introspector
- [ ] Create `src/introspector/fdw.js` — queries `pg_foreign_data_wrapper`, `pg_foreign_server`, `pg_foreign_table`
- [ ] Write `test/fdw.test.js`
- [ ] Commit

### Task 2.21 — User Mappings introspector
- [ ] Create `src/introspector/user-mappings.js` — queries `pg_user_mapping`
- [ ] Write `test/user-mappings.test.js`
- [ ] Commit

### Task 2.22 — Publications introspector
- [ ] Create `src/introspector/publications.js` — queries `pg_publication` + `pg_publication_rel`
- [ ] Write `test/publications.test.js`
- [ ] Commit

### Task 2.23 — Subscriptions introspector
- [ ] Create `src/introspector/subscriptions.js` — queries `pg_subscription`
- [ ] Write `test/subscriptions.test.js`
- [ ] Commit

### Task 2.24 — Event Triggers introspector
- [ ] Create `src/introspector/event-triggers.js` — queries `pg_event_trigger`
- [ ] Write `test/event-triggers.test.js`
- [ ] Commit

### Task 2.25 — Roles introspector
- [ ] Create `src/introspector/roles.js` — queries `pg_roles`, excludes `pg_*` system roles
- [ ] Write `test/roles.test.js`
- [ ] Commit

### Task 2.26 — Grants & Default Privileges introspector
- [ ] Create `src/introspector/grants.js` — uses `aclexplode()` on `pg_class`, `pg_namespace`, `pg_default_acl`
- [ ] Write `test/grants.test.js`
- [ ] Commit

### Task 2.27 — Register all introspectors in orchestrator
- [ ] Update `src/introspector/index.js` to import and register all introspector modules
- [ ] Run all Chunk 2 tests
- [ ] Commit

---

## Chunk 3: Dependency Graph, Differ, SQL Generator & Integration

### Task 3.1 — Dependency Graph
- [ ] Create `src/dependencies.js` with:
  - `queryDependencies(client, schemas)` — queries `pg_depend`
  - `buildGraph(edges)` — directed adjacency list
  - `topologicalSort(graph)` — Kahn's algorithm
  - `reverseTopologicalSort(graph)` — for drop ordering
  - `findDependents(graph, identityKey)` — transitive dependent discovery
  - `buildDependencyInfo(fromClient, toClient, schemas)` — orchestrator
- [ ] Write `test/dependencies.test.js`
- [ ] Commit

### Task 3.2 — Differ
- [ ] Create `src/differ.js` with:
  - `diff(combinedMap, renames, dependencyInfo)` — produces change operations
  - Match by `schema+type+name` with rename overrides
  - Deep compare definitions
  - Select change strategy: CREATE, DROP, ALTER, CREATE_OR_REPLACE, DROP_AND_CREATE
  - Change strategy lookup table per object type
- [ ] Write `test/differ.test.js`
- [ ] Commit

### Task 3.3 — SQL Generator
- [ ] Create `src/sql-generator.js` with:
  - `generateSQL(changes, dependencyInfo)` — takes ordered changes, emits SQL
  - Phase headers (drops commented out, creates/alters, grants)
  - Cascade warnings as comments
  - Timestamp header, no connection info
- [ ] Write `test/sql-generator.test.js`
- [ ] Commit

### Task 3.4 — CLI Integration
- [ ] Update `src/cli.js` to wire the full pipeline:
  - Parse args → connect to both DBs → introspect → build deps → diff → generate SQL → stdout
  - stderr progress messages
  - Handle connection errors gracefully
- [ ] Write `test/integration.test.js`
- [ ] Commit

### Task 3.5 — End-to-End Test
- [ ] Create `test/e2e.test.js`:
  - Create two test databases with different schemas (tables, views, functions, indexes, etc.)
  - Run full pipeline
  - Verify output SQL structure, phase ordering, dependency ordering
  - Verify destructive operations are commented out
- [ ] Commit

---

## Implementation Notes

- **Full code for all introspectors** (SQL queries, DDL generators, tests) is available in the agent output transcripts from the planning phase
- **All SQL must be lowercase**
- **Use `text` type, not `varchar`**
- **Use `rg` (ripgrep), never `grep`**
- Chunk 2 tasks (2.1-2.26) are independent and can be parallelized
- Chunk 3 tasks must be sequential (each builds on the previous)
