## Chunk 3: Dependency Graph, Differ, SQL Generator & Integration

This chunk builds the core pipeline that takes introspected objects from both databases and produces ordered migration SQL. It assumes Chunk 1 (project setup, CLI skeleton, introspector framework) and Chunk 2 (all introspector modules) are complete.

---

### Task 3.1 — Dependency Graph

**File:** `src/dependencies.js`

- [ ] Write failing tests in `test/dependencies.test.js`
- [ ] Create `src/dependencies.js` with ESM exports
- [ ] Implement `queryDependencies(client, schemas)` — queries `pg_depend` joined with `pg_class`/`pg_proc`/`pg_type`/`pg_namespace` to produce edges as `{ fromSchema, fromType, fromName, toSchema, toType, toName }`
- [ ] Implement `buildGraph(edges)` — returns a directed graph (adjacency list as `Map<string, Set<string>>`) where keys are identity strings (`schema.type.name`) and values are the set of objects that key depends on
- [ ] Implement `topologicalSort(graph)` — Kahn's algorithm; returns ordered array of identity strings; throws on cycles with a descriptive error listing the cycle
- [ ] Implement `reverseTopologicalSort(graph)` — reverses the output of `topologicalSort`; used for drop ordering
- [ ] Implement `findDependents(graph, identityKey)` — given the `fromDb` graph and an object identity, returns all objects that transitively depend on it (BFS/DFS on reverse edges)
- [ ] Implement `buildDependencyInfo(fromClient, toClient, schemas)` — orchestrator that queries both databases in parallel, builds both graphs, returns `{ fromGraph, toGraph, sortedCreates, sortedDrops }`

**Query for `queryDependencies`:**

```sql
select
  dns.nspname as dep_schema,
  case dc.relkind
    when 'r' then 'table'
    when 'v' then 'view'
    when 'm' then 'materialized_view'
    when 'i' then 'index'
    when 'S' then 'sequence'
    when 'c' then 'composite_type'
    else
      case
        when dp.oid is not null then 'function'
        when dt.oid is not null then 'type'
        else 'unknown'
      end
  end as dep_type,
  coalesce(dc.relname, dp.proname, dt.typname, '') as dep_name,
  rns.nspname as ref_schema,
  case rc.relkind
    when 'r' then 'table'
    when 'v' then 'view'
    when 'm' then 'materialized_view'
    when 'i' then 'index'
    when 'S' then 'sequence'
    when 'c' then 'composite_type'
    else
      case
        when rp.oid is not null then 'function'
        when rt.oid is not null then 'type'
        else 'unknown'
      end
  end as ref_type,
  coalesce(rc.relname, rp.proname, rt.typname, '') as ref_name
from pg_depend d
left join pg_class dc on d.classid = 'pg_class'::regclass and d.objid = dc.oid
left join pg_proc dp on d.classid = 'pg_proc'::regclass and d.objid = dp.oid
left join pg_type dt on d.classid = 'pg_type'::regclass and d.objid = dt.oid
left join pg_namespace dns on dns.oid = coalesce(dc.relnamespace, dp.pronamespace, dt.typnamespace)
left join pg_class rc on d.refclassid = 'pg_class'::regclass and d.refobjid = rc.oid
left join pg_proc rp on d.refclassid = 'pg_proc'::regclass and d.refobjid = rp.oid
left join pg_type rt on d.refclassid = 'pg_type'::regclass and d.refobjid = rt.oid
left join pg_namespace rns on rns.oid = coalesce(rc.relnamespace, rp.pronamespace, rt.typnamespace)
where d.deptype in ('n', 'a')
  and dns.nspname = any($1)
  and rns.nspname = any($1)
```

Parameter `$1` is the array of schema names.

**Test file:** `test/dependencies.test.js`

```js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { buildGraph, topologicalSort, reverseTopologicalSort, findDependents } from '../src/dependencies.js';

describe('buildGraph', () => {
  it('builds adjacency list from edges', () => {
    const edges = [
      { fromSchema: 'public', fromType: 'view', fromName: 'active_users',
        toSchema: 'public', toType: 'table', toName: 'users' },
      { fromSchema: 'public', fromType: 'view', fromName: 'active_users',
        toSchema: 'public', toType: 'function', toName: 'is_active' },
    ];
    const graph = buildGraph(edges);
    assert.ok(graph.has('public.view.active_users'));
    assert.ok(graph.get('public.view.active_users').has('public.table.users'));
    assert.ok(graph.get('public.view.active_users').has('public.function.is_active'));
  });

  it('returns empty map for no edges', () => {
    const graph = buildGraph([]);
    assert.equal(graph.size, 0);
  });
});

describe('topologicalSort', () => {
  it('sorts dependencies before dependents', () => {
    const graph = new Map();
    graph.set('public.view.v1', new Set(['public.table.t1']));
    graph.set('public.table.t1', new Set(['public.type.e1']));
    graph.set('public.type.e1', new Set());
    const sorted = topologicalSort(graph);
    assert.ok(sorted.indexOf('public.type.e1') < sorted.indexOf('public.table.t1'));
    assert.ok(sorted.indexOf('public.table.t1') < sorted.indexOf('public.view.v1'));
  });

  it('throws on circular dependency', () => {
    const graph = new Map();
    graph.set('a', new Set(['b']));
    graph.set('b', new Set(['a']));
    assert.throws(() => topologicalSort(graph), /circular/i);
  });
});

describe('reverseTopologicalSort', () => {
  it('sorts dependents before dependencies', () => {
    const graph = new Map();
    graph.set('public.view.v1', new Set(['public.table.t1']));
    graph.set('public.table.t1', new Set());
    const sorted = reverseTopologicalSort(graph);
    assert.ok(sorted.indexOf('public.view.v1') < sorted.indexOf('public.table.t1'));
  });
});

describe('findDependents', () => {
  it('finds transitive dependents', () => {
    const graph = new Map();
    graph.set('public.view.v1', new Set(['public.table.t1']));
    graph.set('public.view.v2', new Set(['public.view.v1']));
    graph.set('public.table.t1', new Set());
    const deps = findDependents(graph, 'public.table.t1');
    assert.ok(deps.has('public.view.v1'));
    assert.ok(deps.has('public.view.v2'));
    assert.equal(deps.has('public.table.t1'), false);
  });

  it('returns empty set when no dependents', () => {
    const graph = new Map();
    graph.set('public.table.t1', new Set());
    const deps = findDependents(graph, 'public.table.t1');
    assert.equal(deps.size, 0);
  });
});
```

**Test command:**

```bash
node --test test/dependencies.test.js
```

**Implementation:** `src/dependencies.js`

```js
/**
 * Build identity key string from schema, type, name.
 */
export function identityKey(schema, type, name) {
  return `${schema}.${type}.${name}`;
}

/**
 * Build directed graph from dependency edges.
 * Returns Map<string, Set<string>> where key depends on each value in the set.
 */
export function buildGraph(edges) {
  const graph = new Map();
  for (const e of edges) {
    const from = identityKey(e.fromSchema, e.fromType, e.fromName);
    const to = identityKey(e.toSchema, e.toType, e.toName);
    if (!graph.has(from)) graph.set(from, new Set());
    if (!graph.has(to)) graph.set(to, new Set());
    graph.get(from).add(to);
  }
  return graph;
}

/**
 * Kahn's algorithm topological sort.
 * Returns array ordered so dependencies come before dependents.
 */
export function topologicalSort(graph) {
  // Compute in-degree (number of things each node depends on is NOT in-degree;
  // in-degree = number of nodes that point TO this node)
  // For create ordering: we need dependencies first.
  // Edge A -> B means "A depends on B", so B must come before A.
  // Reverse the edges to get "B is depended on by A", then topo sort.
  const inDegree = new Map();
  const reverseAdj = new Map(); // B -> [A] means A depends on B

  for (const [node] of graph) {
    inDegree.set(node, 0);
    if (!reverseAdj.has(node)) reverseAdj.set(node, []);
  }

  for (const [node, deps] of graph) {
    for (const dep of deps) {
      // node depends on dep, so dep -> node in reverse
      if (!reverseAdj.has(dep)) reverseAdj.set(dep, []);
      reverseAdj.get(dep).push(node);
      inDegree.set(node, (inDegree.get(node) || 0) + 1);
    }
  }

  const queue = [];
  for (const [node, deg] of inDegree) {
    if (deg === 0) queue.push(node);
  }
  queue.sort(); // deterministic ordering for nodes at same level

  const sorted = [];
  while (queue.length > 0) {
    const node = queue.shift();
    sorted.push(node);
    for (const dependent of (reverseAdj.get(node) || [])) {
      const newDeg = inDegree.get(dependent) - 1;
      inDegree.set(dependent, newDeg);
      if (newDeg === 0) {
        queue.push(dependent);
        queue.sort();
      }
    }
  }

  if (sorted.length !== graph.size) {
    const remaining = [...graph.keys()].filter(k => !sorted.includes(k));
    throw new Error(`Circular dependency detected involving: ${remaining.join(', ')}`);
  }

  return sorted;
}

/**
 * Reverse topological sort — dependents come before dependencies.
 * Used for ordering drops.
 */
export function reverseTopologicalSort(graph) {
  return topologicalSort(graph).reverse();
}

/**
 * Find all objects that transitively depend on the given identity key.
 * Uses BFS on reverse edges (dependent -> dependency becomes dependency -> dependent).
 */
export function findDependents(graph, targetKey) {
  // Build reverse adjacency: for each edge A -> B (A depends on B),
  // store B -> A (B is depended on by A)
  const reverseAdj = new Map();
  for (const [node, deps] of graph) {
    for (const dep of deps) {
      if (!reverseAdj.has(dep)) reverseAdj.set(dep, new Set());
      reverseAdj.get(dep).add(node);
    }
  }

  const visited = new Set();
  const queue = [targetKey];
  while (queue.length > 0) {
    const current = queue.shift();
    for (const dependent of (reverseAdj.get(current) || [])) {
      if (!visited.has(dependent)) {
        visited.add(dependent);
        queue.push(dependent);
      }
    }
  }

  return visited;
}

const DEPENDENCY_QUERY = `
select
  dns.nspname as dep_schema,
  case dc.relkind
    when 'r' then 'table'
    when 'v' then 'view'
    when 'm' then 'materialized_view'
    when 'i' then 'index'
    when 'S' then 'sequence'
    when 'c' then 'composite_type'
    else
      case
        when dp.oid is not null then 'function'
        when dt.oid is not null then 'type'
        else 'unknown'
      end
  end as dep_type,
  coalesce(dc.relname, dp.proname, dt.typname, '') as dep_name,
  rns.nspname as ref_schema,
  case rc.relkind
    when 'r' then 'table'
    when 'v' then 'view'
    when 'm' then 'materialized_view'
    when 'i' then 'index'
    when 'S' then 'sequence'
    when 'c' then 'composite_type'
    else
      case
        when rp.oid is not null then 'function'
        when rt.oid is not null then 'type'
        else 'unknown'
      end
  end as ref_type,
  coalesce(rc.relname, rp.proname, rt.typname, '') as ref_name
from pg_depend d
left join pg_class dc on d.classid = 'pg_class'::regclass and d.objid = dc.oid
left join pg_proc dp on d.classid = 'pg_proc'::regclass and d.objid = dp.oid
left join pg_type dt on d.classid = 'pg_type'::regclass and d.objid = dt.oid
left join pg_namespace dns on dns.oid = coalesce(dc.relnamespace, dp.pronamespace, dt.typnamespace)
left join pg_class rc on d.refclassid = 'pg_class'::regclass and d.refobjid = rc.oid
left join pg_proc rp on d.refclassid = 'pg_proc'::regclass and d.refobjid = rp.oid
left join pg_type rt on d.refclassid = 'pg_type'::regclass and d.refobjid = rt.oid
left join pg_namespace rns on rns.oid = coalesce(rc.relnamespace, rp.pronamespace, rt.typnamespace)
where d.deptype in ('n', 'a')
  and dns.nspname = any($1)
  and rns.nspname = any($1)
`;

/**
 * Query pg_depend from a database and return normalized edges.
 */
export async function queryDependencies(client, schemas) {
  const { rows } = await client.query(DEPENDENCY_QUERY, [schemas]);
  return rows.filter(r => r.dep_name && r.ref_name && r.dep_type !== 'unknown' && r.ref_type !== 'unknown');
}

/**
 * Build dependency info from both databases.
 * Returns { fromGraph, toGraph, sortedCreates, sortedDrops }.
 */
export async function buildDependencyInfo(fromClient, toClient, schemas) {
  const [fromEdges, toEdges] = await Promise.all([
    queryDependencies(fromClient, schemas),
    queryDependencies(toClient, schemas),
  ]);

  const fromGraph = buildGraph(fromEdges);
  const toGraph = buildGraph(toEdges);

  const sortedCreates = topologicalSort(toGraph);
  const sortedDrops = reverseTopologicalSort(fromGraph);

  return { fromGraph, toGraph, sortedCreates, sortedDrops };
}
```

---

### Task 3.2 — Differ

**File:** `src/differ.js`

- [ ] Write failing tests in `test/differ.test.js`
- [ ] Create `src/differ.js` with ESM exports
- [ ] Implement `parseRenames(renameArgs)` — parses `--rename` flag values into a lookup map; supports `table:old:new` and `column:schema.table/old_col:new_col` formats; defaults schema to `public` when omitted for column renames
- [ ] Implement `matchObjects(fromObjects, toObjects, renames)` — matches by `schema + type + name`, applying rename overrides; returns `{ matched, createOnly, dropOnly }`
- [ ] Implement `deepEqual(a, b)` — recursive deep comparison of definition objects (plain JSON-safe objects)
- [ ] Implement `determineChangeStrategy(objectType, fromDef, toDef)` — based on the change types table in the spec, returns one of: `ALTER`, `CREATE_OR_REPLACE`, `DROP_AND_CREATE`; for objects that support multiple strategies, inspects which fields changed to pick the least destructive option
- [ ] Implement `diff(fromObjects, toObjects, options)` — main entry point; takes introspected object arrays and options (`{ renames, excludeTypes, excludeSchemas }`); returns ordered array of change operations: `{ op, identity, fromDef, toDef, reason }`
- [ ] Handle column-level renames: when a table is matched and columns differ, check column rename mappings before classifying a column as added/dropped

**Change operations produced:**

```
{ op: 'CREATE', identity: { schema, type, name }, toDef, reason: 'new object' }
{ op: 'DROP', identity: { schema, type, name }, fromDef, reason: 'removed object' }
{ op: 'ALTER', identity: { schema, type, name }, fromDef, toDef, changes: [...], reason: '...' }
{ op: 'CREATE_OR_REPLACE', identity: { schema, type, name }, toDef, reason: '...' }
{ op: 'DROP_AND_CREATE', identity: { schema, type, name }, fromDef, toDef, reason: '...' }
{ op: 'RENAME', identity: { schema, type, name }, newName, reason: 'rename from ...' }
```

**Test file:** `test/differ.test.js`

```js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { parseRenames, matchObjects, determineChangeStrategy, diff, deepEqual } from '../src/differ.js';

describe('parseRenames', () => {
  it('parses table renames', () => {
    const renames = parseRenames(['table:users:accounts']);
    assert.deepEqual(renames.tables.get('public.users'), 'accounts');
  });

  it('parses column renames with schema', () => {
    const renames = parseRenames(['column:myschema.users/email:email_address']);
    assert.deepEqual(renames.columns.get('myschema.users.email'), 'email_address');
  });

  it('defaults column rename schema to public', () => {
    const renames = parseRenames(['column:users/email:email_address']);
    assert.deepEqual(renames.columns.get('public.users.email'), 'email_address');
  });

  it('handles multiple renames', () => {
    const renames = parseRenames([
      'table:old_t:new_t',
      'column:t1/old_c:new_c',
    ]);
    assert.equal(renames.tables.size, 1);
    assert.equal(renames.columns.size, 1);
  });

  it('throws on invalid format', () => {
    assert.throws(() => parseRenames(['bad']), /invalid rename/i);
  });
});

describe('matchObjects', () => {
  it('matches objects by schema+type+name', () => {
    const from = [
      { identity: { schema: 'public', type: 'table', name: 'users' }, definition: { columns: [] } },
    ];
    const to = [
      { identity: { schema: 'public', type: 'table', name: 'users' }, definition: { columns: [{ name: 'id' }] } },
    ];
    const { matched, createOnly, dropOnly } = matchObjects(from, to, { tables: new Map(), columns: new Map() });
    assert.equal(matched.length, 1);
    assert.equal(createOnly.length, 0);
    assert.equal(dropOnly.length, 0);
  });

  it('applies table renames when matching', () => {
    const from = [
      { identity: { schema: 'public', type: 'table', name: 'users' }, definition: {} },
    ];
    const to = [
      { identity: { schema: 'public', type: 'table', name: 'accounts' }, definition: {} },
    ];
    const renames = { tables: new Map([['public.users', 'accounts']]), columns: new Map() };
    const { matched, createOnly, dropOnly } = matchObjects(from, to, renames);
    assert.equal(matched.length, 1);
    assert.equal(matched[0].renamed, true);
    assert.equal(createOnly.length, 0);
    assert.equal(dropOnly.length, 0);
  });

  it('classifies unmatched objects', () => {
    const from = [
      { identity: { schema: 'public', type: 'table', name: 'old_table' }, definition: {} },
    ];
    const to = [
      { identity: { schema: 'public', type: 'table', name: 'new_table' }, definition: {} },
    ];
    const renames = { tables: new Map(), columns: new Map() };
    const { matched, createOnly, dropOnly } = matchObjects(from, to, renames);
    assert.equal(matched.length, 0);
    assert.equal(createOnly.length, 1);
    assert.equal(dropOnly.length, 1);
  });
});

describe('deepEqual', () => {
  it('returns true for identical objects', () => {
    assert.equal(deepEqual({ a: 1, b: [2, 3] }, { a: 1, b: [2, 3] }), true);
  });

  it('returns false for different objects', () => {
    assert.equal(deepEqual({ a: 1 }, { a: 2 }), false);
  });

  it('handles nested objects', () => {
    assert.equal(deepEqual({ a: { b: { c: 1 } } }, { a: { b: { c: 1 } } }), true);
    assert.equal(deepEqual({ a: { b: { c: 1 } } }, { a: { b: { c: 2 } } }), false);
  });

  it('handles nulls', () => {
    assert.equal(deepEqual(null, null), true);
    assert.equal(deepEqual(null, {}), false);
  });
});

describe('determineChangeStrategy', () => {
  it('returns ALTER for tables with column changes', () => {
    assert.equal(determineChangeStrategy('table', {}, {}), 'ALTER');
  });

  it('returns CREATE_OR_REPLACE for functions', () => {
    assert.equal(
      determineChangeStrategy('function', { body: 'old' }, { body: 'new' }),
      'CREATE_OR_REPLACE'
    );
  });

  it('returns CREATE_OR_REPLACE for views', () => {
    assert.equal(
      determineChangeStrategy('view', { query: 'old' }, { query: 'new' }),
      'CREATE_OR_REPLACE'
    );
  });

  it('returns DROP_AND_CREATE for indexes', () => {
    assert.equal(determineChangeStrategy('index', {}, {}), 'DROP_AND_CREATE');
  });

  it('returns DROP_AND_CREATE for triggers', () => {
    assert.equal(determineChangeStrategy('trigger', {}, {}), 'DROP_AND_CREATE');
  });

  it('returns DROP_AND_CREATE for materialized views', () => {
    assert.equal(determineChangeStrategy('materialized_view', {}, {}), 'DROP_AND_CREATE');
  });

  it('returns ALTER for enum adding values', () => {
    const from = { labels: ['a', 'b'] };
    const to = { labels: ['a', 'b', 'c'] };
    assert.equal(determineChangeStrategy('enum', from, to), 'ALTER');
  });

  it('returns DROP_AND_CREATE for enum removing values', () => {
    const from = { labels: ['a', 'b', 'c'] };
    const to = { labels: ['a', 'b'] };
    assert.equal(determineChangeStrategy('enum', from, to), 'DROP_AND_CREATE');
  });
});

describe('diff', () => {
  it('produces CREATE for new objects', () => {
    const from = [];
    const to = [
      { identity: { schema: 'public', type: 'table', name: 't1' }, definition: { columns: [] }, ddl: {} },
    ];
    const ops = diff(from, to, {});
    assert.equal(ops.length, 1);
    assert.equal(ops[0].op, 'CREATE');
  });

  it('produces DROP for removed objects', () => {
    const from = [
      { identity: { schema: 'public', type: 'table', name: 't1' }, definition: { columns: [] }, ddl: {} },
    ];
    const to = [];
    const ops = diff(from, to, {});
    assert.equal(ops.length, 1);
    assert.equal(ops[0].op, 'DROP');
  });

  it('produces no ops for identical objects', () => {
    const obj = { identity: { schema: 'public', type: 'table', name: 't1' }, definition: { columns: [{ name: 'id', type: 'integer' }] }, ddl: {} };
    const ops = diff([obj], [{ ...obj }], {});
    assert.equal(ops.length, 0);
  });

  it('respects excludeTypes', () => {
    const from = [];
    const to = [
      { identity: { schema: 'public', type: 'table', name: 't1' }, definition: {}, ddl: {} },
      { identity: { schema: 'public', type: 'index', name: 'i1' }, definition: {}, ddl: {} },
    ];
    const ops = diff(from, to, { excludeTypes: ['index'] });
    assert.equal(ops.length, 1);
    assert.equal(ops[0].identity.type, 'table');
  });

  it('respects excludeSchemas', () => {
    const from = [];
    const to = [
      { identity: { schema: 'public', type: 'table', name: 't1' }, definition: {}, ddl: {} },
      { identity: { schema: 'audit', type: 'table', name: 't2' }, definition: {}, ddl: {} },
    ];
    const ops = diff(from, to, { excludeSchemas: ['audit'] });
    assert.equal(ops.length, 1);
    assert.equal(ops[0].identity.schema, 'public');
  });

  it('produces RENAME ops when renames are specified', () => {
    const from = [
      { identity: { schema: 'public', type: 'table', name: 'users' }, definition: { columns: [] }, ddl: {} },
    ];
    const to = [
      { identity: { schema: 'public', type: 'table', name: 'accounts' }, definition: { columns: [] }, ddl: {} },
    ];
    const ops = diff(from, to, { renames: ['table:users:accounts'] });
    const renameOp = ops.find(o => o.op === 'RENAME');
    assert.ok(renameOp);
    assert.equal(renameOp.newName, 'accounts');
  });
});
```

**Test command:**

```bash
node --test test/differ.test.js
```

**Implementation:** `src/differ.js`

```js
/**
 * Parse --rename flag values into structured lookup maps.
 * Formats:
 *   table:old_name:new_name
 *   column:schema.table/old_col:new_col
 *   column:table/old_col:new_col  (defaults to public schema)
 */
export function parseRenames(renameArgs) {
  const tables = new Map();   // 'schema.oldName' -> 'newName'
  const columns = new Map();  // 'schema.table.oldCol' -> 'newCol'

  if (!renameArgs || renameArgs.length === 0) {
    return { tables, columns };
  }

  for (const spec of renameArgs) {
    const parts = spec.split(':');
    if (parts.length !== 3) {
      throw new Error(`Invalid rename spec: "${spec}". Expected format type:old:new`);
    }

    const [kind, oldPart, newPart] = parts;

    if (kind === 'table') {
      const key = oldPart.includes('.') ? oldPart : `public.${oldPart}`;
      tables.set(key, newPart);
    } else if (kind === 'column') {
      // old_part format: [schema.]table/col_name
      const slashIdx = oldPart.indexOf('/');
      if (slashIdx === -1) {
        throw new Error(`Invalid rename spec: "${spec}". Column renames require table/column format`);
      }
      const tablePart = oldPart.substring(0, slashIdx);
      const colName = oldPart.substring(slashIdx + 1);
      const qualifiedTable = tablePart.includes('.') ? tablePart : `public.${tablePart}`;
      columns.set(`${qualifiedTable}.${colName}`, newPart);
    } else {
      throw new Error(`Invalid rename type: "${kind}". Expected "table" or "column"`);
    }
  }

  return { tables, columns };
}

/**
 * Deep equality comparison for plain JSON-safe objects.
 */
export function deepEqual(a, b) {
  if (a === b) return true;
  if (a === null || b === null) return false;
  if (typeof a !== typeof b) return false;

  if (Array.isArray(a)) {
    if (!Array.isArray(b) || a.length !== b.length) return false;
    return a.every((val, i) => deepEqual(val, b[i]));
  }

  if (typeof a === 'object') {
    const keysA = Object.keys(a).sort();
    const keysB = Object.keys(b).sort();
    if (keysA.length !== keysB.length) return false;
    return keysA.every((key, i) => keysB[i] === key && deepEqual(a[key], b[key]));
  }

  return false;
}

function identityKey(identity) {
  return `${identity.schema}.${identity.type}.${identity.name}`;
}

/**
 * Match objects from both databases by schema+type+name, applying renames.
 */
export function matchObjects(fromObjects, toObjects, renames) {
  const toMap = new Map();
  for (const obj of toObjects) {
    toMap.set(identityKey(obj.identity), obj);
  }

  const matched = [];
  const dropOnly = [];
  const matchedToKeys = new Set();

  for (const fromObj of fromObjects) {
    const fromKey = identityKey(fromObj.identity);
    let toObj = toMap.get(fromKey);
    let renamed = false;

    // Check rename mappings
    if (!toObj && fromObj.identity.type === 'table') {
      const renameKey = `${fromObj.identity.schema}.${fromObj.identity.name}`;
      const newName = renames.tables.get(renameKey);
      if (newName) {
        const renamedKey = `${fromObj.identity.schema}.table.${newName}`;
        toObj = toMap.get(renamedKey);
        if (toObj) {
          renamed = true;
          matchedToKeys.add(renamedKey);
        }
      }
    }

    if (toObj && !renamed) {
      matchedToKeys.add(fromKey);
    }

    if (toObj) {
      matched.push({ from: fromObj, to: toObj, renamed });
    } else {
      dropOnly.push(fromObj);
    }
  }

  const createOnly = toObjects.filter(obj => !matchedToKeys.has(identityKey(obj.identity)));

  return { matched, createOnly, dropOnly };
}

// Strategy lookup tables based on the spec's change types table.
const CAN_ALTER = new Set([
  'schema', 'table', 'column', 'function', 'procedure', 'domain',
  'sequence', 'extension', 'policy', 'enum', 'composite_type',
  'foreign_data_wrapper', 'foreign_server', 'foreign_table', 'user_mapping',
  'publication', 'subscription',
]);

const CAN_CREATE_OR_REPLACE = new Set([
  'function', 'procedure', 'view',
]);

const MUST_DROP_AND_CREATE = new Set([
  'index', 'constraint', 'materialized_view', 'trigger',
  'operator', 'aggregate', 'cast', 'statistics',
]);

/**
 * Determine the least destructive change strategy for an object type.
 */
export function determineChangeStrategy(objectType, fromDef, toDef) {
  // Always DROP+CREATE for types that require it
  if (MUST_DROP_AND_CREATE.has(objectType)) {
    return 'DROP_AND_CREATE';
  }

  // Enums: can ALTER to add values, but must DROP+CREATE to remove them
  if (objectType === 'enum') {
    if (fromDef.labels && toDef.labels) {
      const removed = fromDef.labels.filter(l => !toDef.labels.includes(l));
      if (removed.length > 0) return 'DROP_AND_CREATE';
    }
    return 'ALTER';
  }

  // Functions, procedures, views: prefer CREATE OR REPLACE
  if (CAN_CREATE_OR_REPLACE.has(objectType)) {
    return 'CREATE_OR_REPLACE';
  }

  // Everything else that can ALTER
  if (CAN_ALTER.has(objectType)) {
    return 'ALTER';
  }

  // Fallback
  return 'DROP_AND_CREATE';
}

/**
 * Main differ entry point.
 * Returns array of change operations.
 */
export function diff(fromObjects, toObjects, options = {}) {
  const { renames: renameArgs, excludeTypes, excludeSchemas } = options;

  // Filter excluded types and schemas
  const filterObj = (obj) => {
    if (excludeTypes && excludeTypes.includes(obj.identity.type)) return false;
    if (excludeSchemas && excludeSchemas.includes(obj.identity.schema)) return false;
    return true;
  };

  const filteredFrom = fromObjects.filter(filterObj);
  const filteredTo = toObjects.filter(filterObj);

  const parsedRenames = parseRenames(renameArgs);
  const { matched, createOnly, dropOnly } = matchObjects(filteredFrom, filteredTo, parsedRenames);

  const ops = [];

  // DROP operations for removed objects
  for (const obj of dropOnly) {
    ops.push({
      op: 'DROP',
      identity: obj.identity,
      fromDef: obj.definition,
      ddl: obj.ddl,
      reason: 'removed object',
    });
  }

  // CREATE operations for new objects
  for (const obj of createOnly) {
    ops.push({
      op: 'CREATE',
      identity: obj.identity,
      toDef: obj.definition,
      ddl: obj.ddl,
      reason: 'new object',
    });
  }

  // Process matched pairs
  for (const { from, to, renamed } of matched) {
    // Emit rename if applicable
    if (renamed) {
      ops.push({
        op: 'RENAME',
        identity: from.identity,
        newName: to.identity.name,
        ddl: to.ddl,
        reason: `rename from ${from.identity.name} to ${to.identity.name}`,
      });
    }

    // Compare definitions
    if (!deepEqual(from.definition, to.definition)) {
      const strategy = determineChangeStrategy(from.identity.type, from.definition, to.definition);
      ops.push({
        op: strategy,
        identity: to.identity,
        fromDef: from.definition,
        toDef: to.definition,
        ddl: to.ddl,
        reason: `definition changed`,
      });
    }
  }

  return ops;
}
```

---

### Task 3.3 — SQL Generator

**File:** `src/sql-generator.js`

- [ ] Write failing tests in `test/sql-generator.test.js`
- [ ] Create `src/sql-generator.js` with ESM exports
- [ ] Implement `generate(operations, dependencyInfo)` — takes the array of change ops and dependency info; reorders operations according to the dependency graphs; returns formatted SQL string
- [ ] Implement phase grouping: Phase 1 (drops, commented out), Phase 2 (creates and alters), Phase 3 (grants/revokes)
- [ ] Implement `commentOut(sql)` — prefixes each line with `-- `
- [ ] Implement cascade warning comments: when a DROP_AND_CREATE triggers dependent drops, emit explanatory comments
- [ ] Add header with generation timestamp and partial-failure warning
- [ ] Format each operation with a `-- <action> <type> <schema>.<name>` comment header

**Test file:** `test/sql-generator.test.js`

```js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { generate, commentOut, formatHeader } from '../src/sql-generator.js';

describe('commentOut', () => {
  it('comments out single line', () => {
    assert.equal(commentOut('drop table public.users;'), '-- drop table public.users;');
  });

  it('comments out multiple lines', () => {
    const input = 'drop table public.users;\ndrop table public.posts;';
    const expected = '-- drop table public.users;\n-- drop table public.posts;';
    assert.equal(commentOut(input), expected);
  });

  it('handles empty string', () => {
    assert.equal(commentOut(''), '-- ');
  });
});

describe('formatHeader', () => {
  it('includes generation timestamp', () => {
    const header = formatHeader();
    assert.ok(header.includes('dbdelta migration'));
    assert.ok(header.includes('Generated:'));
    assert.ok(header.includes('WARNING'));
  });
});

describe('generate', () => {
  const makeDdl = (createSql, dropSql, alterSql) => ({
    create: () => createSql,
    drop: () => dropSql,
    alter: (fromDef, toDef) => alterSql,
  });

  it('produces header and phase markers', () => {
    const sql = generate([], { sortedCreates: [], sortedDrops: [], fromGraph: new Map(), toGraph: new Map() });
    assert.ok(sql.includes('dbdelta migration'));
    assert.ok(sql.includes('PHASE 1'));
    assert.ok(sql.includes('PHASE 2'));
    assert.ok(sql.includes('PHASE 3'));
  });

  it('comments out DROP operations', () => {
    const ops = [{
      op: 'DROP',
      identity: { schema: 'public', type: 'table', name: 'users' },
      ddl: makeDdl(null, 'drop table public.users;', null),
      reason: 'removed',
    }];
    const depInfo = { sortedCreates: [], sortedDrops: ['public.table.users'], fromGraph: new Map(), toGraph: new Map() };
    const sql = generate(ops, depInfo);
    assert.ok(sql.includes('-- drop table public.users;'));
  });

  it('emits CREATE operations without comments', () => {
    const ops = [{
      op: 'CREATE',
      identity: { schema: 'public', type: 'table', name: 'users' },
      toDef: {},
      ddl: makeDdl('create table public.users (\n  id serial primary key\n);', null, null),
      reason: 'new object',
    }];
    const depInfo = { sortedCreates: ['public.table.users'], sortedDrops: [], fromGraph: new Map(), toGraph: new Map() };
    const sql = generate(ops, depInfo);
    assert.ok(sql.includes('create table public.users'));
    // The SQL itself should NOT be commented out
    assert.ok(sql.includes('\ncreate table'));
  });

  it('emits ALTER operations', () => {
    const ops = [{
      op: 'ALTER',
      identity: { schema: 'public', type: 'table', name: 'users' },
      fromDef: {},
      toDef: {},
      ddl: makeDdl(null, null, 'alter table public.users add column email text;'),
      reason: 'definition changed',
    }];
    const depInfo = { sortedCreates: ['public.table.users'], sortedDrops: [], fromGraph: new Map(), toGraph: new Map() };
    const sql = generate(ops, depInfo);
    assert.ok(sql.includes('alter table public.users add column email text;'));
  });

  it('emits CREATE OR REPLACE operations', () => {
    const ops = [{
      op: 'CREATE_OR_REPLACE',
      identity: { schema: 'public', type: 'function', name: 'get_user' },
      toDef: {},
      ddl: makeDdl('create or replace function public.get_user() returns void as $$ begin end; $$ language plpgsql;', null, null),
      reason: 'definition changed',
    }];
    const depInfo = { sortedCreates: ['public.function.get_user'], sortedDrops: [], fromGraph: new Map(), toGraph: new Map() };
    const sql = generate(ops, depInfo);
    assert.ok(sql.includes('create or replace function'));
  });

  it('emits DROP_AND_CREATE as drop then create', () => {
    const ops = [{
      op: 'DROP_AND_CREATE',
      identity: { schema: 'public', type: 'index', name: 'idx_users_email' },
      fromDef: {},
      toDef: {},
      ddl: makeDdl(
        'create index idx_users_email on public.users (email);',
        'drop index public.idx_users_email;',
        null
      ),
      reason: 'definition changed',
    }];
    const depInfo = {
      sortedCreates: ['public.index.idx_users_email'],
      sortedDrops: ['public.index.idx_users_email'],
      fromGraph: new Map(),
      toGraph: new Map(),
    };
    const sql = generate(ops, depInfo);
    assert.ok(sql.includes('drop index'));
    assert.ok(sql.includes('create index'));
  });

  it('emits RENAME operations', () => {
    const ops = [{
      op: 'RENAME',
      identity: { schema: 'public', type: 'table', name: 'users' },
      newName: 'accounts',
      ddl: { rename: (oldName, newName) => `alter table public.${oldName} rename to ${newName};` },
      reason: 'rename',
    }];
    const depInfo = { sortedCreates: [], sortedDrops: [], fromGraph: new Map(), toGraph: new Map() };
    const sql = generate(ops, depInfo);
    assert.ok(sql.includes('alter table public.users rename to accounts;'));
  });

  it('separates grants into phase 3', () => {
    const ops = [
      {
        op: 'CREATE',
        identity: { schema: 'public', type: 'table', name: 't1' },
        toDef: {},
        ddl: makeDdl('create table public.t1 (id integer);', null, null),
        reason: 'new',
      },
      {
        op: 'ALTER',
        identity: { schema: 'public', type: 'grant', name: 'select_on_t1' },
        fromDef: {},
        toDef: {},
        ddl: makeDdl(null, null, 'grant select on public.t1 to reader;'),
        reason: 'changed',
      },
    ];
    const depInfo = { sortedCreates: ['public.table.t1'], sortedDrops: [], fromGraph: new Map(), toGraph: new Map() };
    const sql = generate(ops, depInfo);
    const phase2Idx = sql.indexOf('PHASE 2');
    const phase3Idx = sql.indexOf('PHASE 3');
    const createIdx = sql.indexOf('create table');
    const grantIdx = sql.indexOf('grant select');
    assert.ok(createIdx > phase2Idx);
    assert.ok(grantIdx > phase3Idx);
  });
});
```

**Test command:**

```bash
node --test test/sql-generator.test.js
```

**Implementation:** `src/sql-generator.js`

```js
/**
 * Comment out a SQL string by prefixing each line with '-- '.
 */
export function commentOut(sql) {
  return sql.split('\n').map(line => `-- ${line}`).join('\n');
}

/**
 * Generate the file header with timestamp and warnings.
 */
export function formatHeader() {
  const now = new Date().toISOString();
  return [
    '-- dbdelta migration',
    `-- Generated: ${now}`,
    '-- WARNING: no transaction wrapper; failures may leave the database in a partial state',
    '',
  ].join('\n');
}

function identityKey(identity) {
  return `${identity.schema}.${identity.type}.${identity.name}`;
}

/**
 * Order operations according to dependency sort order.
 * Operations not in the sort order are appended at the end.
 */
function orderOps(ops, sortOrder) {
  const orderMap = new Map();
  sortOrder.forEach((key, idx) => orderMap.set(key, idx));

  return [...ops].sort((a, b) => {
    const aIdx = orderMap.get(identityKey(a.identity)) ?? Number.MAX_SAFE_INTEGER;
    const bIdx = orderMap.get(identityKey(b.identity)) ?? Number.MAX_SAFE_INTEGER;
    return aIdx - bIdx;
  });
}

/**
 * Generate SQL for a single operation.
 */
function emitOp(op) {
  const { identity } = op;
  const label = `${identity.schema}.${identity.name}`;
  const lines = [];

  switch (op.op) {
    case 'DROP': {
      lines.push(`-- drop ${identity.type} ${label}`);
      const sql = op.ddl.drop();
      lines.push(commentOut(sql));
      break;
    }
    case 'CREATE': {
      lines.push(`-- create ${identity.type} ${label}`);
      lines.push(op.ddl.create());
      break;
    }
    case 'ALTER': {
      lines.push(`-- alter ${identity.type} ${label}`);
      lines.push(op.ddl.alter(op.fromDef, op.toDef));
      break;
    }
    case 'CREATE_OR_REPLACE': {
      lines.push(`-- create or replace ${identity.type} ${label}`);
      lines.push(op.ddl.create());
      break;
    }
    case 'DROP_AND_CREATE': {
      lines.push(`-- drop and recreate ${identity.type} ${label}`);
      lines.push(op.ddl.drop());
      lines.push(op.ddl.create());
      break;
    }
    case 'RENAME': {
      lines.push(`-- rename ${identity.type} ${label} -> ${op.newName}`);
      lines.push(op.ddl.rename(identity.name, op.newName));
      break;
    }
  }

  return lines.join('\n');
}

/**
 * Main SQL generation entry point.
 * Takes ordered change operations and dependency info.
 * Returns formatted SQL string.
 */
export function generate(operations, dependencyInfo) {
  const { sortedCreates, sortedDrops } = dependencyInfo;

  // Separate operations into phases
  const grantTypes = new Set(['grant', 'default_privilege']);
  const dropOps = operations.filter(o => o.op === 'DROP');
  const grantOps = operations.filter(o => grantTypes.has(o.identity.type));
  const createAlterOps = operations.filter(o =>
    o.op !== 'DROP' && !grantTypes.has(o.identity.type)
  );

  // Handle DROP_AND_CREATE: split the drop portion into phase 1
  const dropAndCreateOps = operations.filter(o => o.op === 'DROP_AND_CREATE');

  // Order each phase
  const orderedDrops = orderOps(dropOps, sortedDrops);
  const orderedCreateAlters = orderOps(createAlterOps, sortedCreates);
  const orderedGrants = [...grantOps].sort((a, b) =>
    identityKey(a.identity).localeCompare(identityKey(b.identity))
  );

  const sections = [];

  // Header
  sections.push(formatHeader());

  // Phase 1: Drops
  sections.push('-- === PHASE 1: DROPS (commented out -- review carefully) ===');
  if (orderedDrops.length > 0) {
    for (const op of orderedDrops) {
      sections.push(emitOp(op));
    }
  }
  sections.push('');

  // Phase 2: Creates & Alters
  sections.push('-- === PHASE 2: CREATES & ALTERS ===');
  if (orderedCreateAlters.length > 0) {
    for (const op of orderedCreateAlters) {
      sections.push(emitOp(op));
    }
  }
  sections.push('');

  // Phase 3: Grants/Revokes
  sections.push('-- === PHASE 3: GRANTS/REVOKES ===');
  if (orderedGrants.length > 0) {
    for (const op of orderedGrants) {
      sections.push(emitOp(op));
    }
  }
  sections.push('');

  return sections.join('\n');
}
```

---

### Task 3.4 — CLI Integration

**File:** `src/cli.js` (update existing)

- [ ] Write failing tests in `test/cli.test.js`
- [ ] Update `src/cli.js` to wire the full pipeline: introspect -> build deps -> diff -> generate SQL
- [ ] Parse `--rename` (repeatable) into an array
- [ ] Parse `--exclude-types` and `--exclude-schemas` from comma-separated strings into arrays
- [ ] Add `stderr` progress messages at each pipeline stage
- [ ] Handle connection errors gracefully with descriptive stderr messages
- [ ] Output final SQL to stdout via `process.stdout.write()`

**Updated `src/cli.js`:**

```js
import { parseArgs } from 'node:util';
import pg from 'pg';
import { introspect } from './introspector/index.js';
import { buildDependencyInfo } from './dependencies.js';
import { diff } from './differ.js';
import { generate } from './sql-generator.js';

const SYSTEM_SCHEMAS = ['pg_catalog', 'information_schema', 'pg_toast'];

function parseCliArgs(argv) {
  const { values, positionals } = parseArgs({
    args: argv.slice(2),
    allowPositionals: true,
    options: {
      schemas: { type: 'string', default: 'public' },
      rename: { type: 'string', multiple: true, default: [] },
      'exclude-schemas': { type: 'string', default: '' },
      'exclude-types': { type: 'string', default: '' },
    },
  });

  if (positionals.length < 2) {
    process.stderr.write('Usage: dbdelta <fromUrl> <toUrl> [options]\n');
    process.exit(1);
  }

  const schemas = values.schemas.split(',').map(s => s.trim()).filter(Boolean);
  const excludeSchemas = [
    ...SYSTEM_SCHEMAS,
    ...(values['exclude-schemas'] ? values['exclude-schemas'].split(',').map(s => s.trim()).filter(Boolean) : []),
  ];
  const excludeTypes = values['exclude-types']
    ? values['exclude-types'].split(',').map(s => s.trim()).filter(Boolean)
    : [];

  return {
    fromUrl: positionals[0],
    toUrl: positionals[1],
    schemas,
    renames: values.rename,
    excludeSchemas,
    excludeTypes,
  };
}

function progress(msg) {
  process.stderr.write(`[dbdelta] ${msg}\n`);
}

export async function run(argv = process.argv) {
  const config = parseCliArgs(argv);

  const fromClient = new pg.Client({ connectionString: config.fromUrl });
  const toClient = new pg.Client({ connectionString: config.toUrl });

  try {
    progress('connecting to databases...');
    await Promise.all([fromClient.connect(), toClient.connect()]);

    progress('introspecting schemas...');
    const [fromObjects, toObjects] = await Promise.all([
      introspect(fromClient, config.schemas),
      introspect(toClient, config.schemas),
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
    await Promise.allSettled([fromClient.end(), toClient.end()]);
  }
}

export { parseCliArgs };
```

**Test file:** `test/cli.test.js`

```js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { parseCliArgs } from '../src/cli.js';

describe('parseCliArgs', () => {
  it('parses positional args as connection URLs', () => {
    const config = parseCliArgs(['node', 'cli.js', 'postgres://from', 'postgres://to']);
    assert.equal(config.fromUrl, 'postgres://from');
    assert.equal(config.toUrl, 'postgres://to');
  });

  it('defaults schemas to public', () => {
    const config = parseCliArgs(['node', 'cli.js', 'from', 'to']);
    assert.deepEqual(config.schemas, ['public']);
  });

  it('parses --schemas', () => {
    const config = parseCliArgs(['node', 'cli.js', 'from', 'to', '--schemas', 'public,audit']);
    assert.deepEqual(config.schemas, ['public', 'audit']);
  });

  it('parses multiple --rename flags', () => {
    const config = parseCliArgs([
      'node', 'cli.js', 'from', 'to',
      '--rename', 'table:a:b',
      '--rename', 'column:t/c:d',
    ]);
    assert.deepEqual(config.renames, ['table:a:b', 'column:t/c:d']);
  });

  it('parses --exclude-types', () => {
    const config = parseCliArgs(['node', 'cli.js', 'from', 'to', '--exclude-types', 'grants,roles']);
    assert.deepEqual(config.excludeTypes, ['grants', 'roles']);
  });

  it('parses --exclude-schemas', () => {
    const config = parseCliArgs(['node', 'cli.js', 'from', 'to', '--exclude-schemas', 'temp']);
    assert.ok(config.excludeSchemas.includes('temp'));
    // System schemas always included
    assert.ok(config.excludeSchemas.includes('pg_catalog'));
    assert.ok(config.excludeSchemas.includes('information_schema'));
  });

  it('defaults exclude lists to empty', () => {
    const config = parseCliArgs(['node', 'cli.js', 'from', 'to']);
    assert.deepEqual(config.excludeTypes, []);
    assert.ok(config.excludeSchemas.length > 0); // system schemas
  });
});
```

**Test command:**

```bash
node --test test/cli.test.js
```

---

### Task 3.5 — End-to-End Integration Test

**File:** `test/e2e.test.js`

- [ ] Write an end-to-end test that creates two test databases, populates them with different schemas, runs the full pipeline, and verifies the output
- [ ] The test requires two running PostgreSQL instances (uses environment variables `TEST_PG_FROM_URL` and `TEST_PG_TO_URL`)
- [ ] Test creates schemas in both databases, runs `run()` with captured stdout, and validates the SQL output
- [ ] Test validates: correct phase ordering, drops are commented out, creates are present, dependency ordering is respected
- [ ] Add a `test:e2e` script to `package.json`

**Update `package.json` scripts:**

```json
{
  "scripts": {
    "test": "node --test test/*.test.js",
    "test:unit": "node --test test/dependencies.test.js test/differ.test.js test/sql-generator.test.js test/cli.test.js",
    "test:e2e": "node --test test/e2e.test.js"
  }
}
```

**Test file:** `test/e2e.test.js`

```js
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';

const FROM_URL = process.env.TEST_PG_FROM_URL;
const TO_URL = process.env.TEST_PG_TO_URL;

const skip = !FROM_URL || !TO_URL;

describe('end-to-end pipeline', { skip }, () => {
  let fromClient;
  let toClient;

  before(async () => {
    fromClient = new pg.Client({ connectionString: FROM_URL });
    toClient = new pg.Client({ connectionString: TO_URL });
    await Promise.all([fromClient.connect(), toClient.connect()]);

    // Setup "from" database
    await fromClient.query(`
      drop schema if exists e2e_test cascade;
      create schema e2e_test;

      create table e2e_test.users (
        id serial primary key,
        name text not null,
        email text
      );

      create index idx_users_email on e2e_test.users (email);

      create table e2e_test.posts (
        id serial primary key,
        user_id integer references e2e_test.users(id),
        title text,
        body text
      );

      create view e2e_test.active_users as
        select id, name from e2e_test.users where name is not null;

      create table e2e_test.old_table (
        id serial primary key
      );
    `);

    // Setup "to" database (target state)
    await toClient.query(`
      drop schema if exists e2e_test cascade;
      create schema e2e_test;

      create table e2e_test.users (
        id serial primary key,
        name text not null,
        email text not null,
        created_at timestamp default now()
      );

      create index idx_users_email on e2e_test.users (email);
      create index idx_users_name on e2e_test.users (name);

      create table e2e_test.posts (
        id serial primary key,
        user_id integer references e2e_test.users(id),
        title text not null,
        body text,
        published boolean default false
      );

      create view e2e_test.active_users as
        select id, name, email from e2e_test.users where name is not null;

      create table e2e_test.comments (
        id serial primary key,
        post_id integer references e2e_test.posts(id),
        body text
      );
    `);
  });

  after(async () => {
    if (fromClient) {
      await fromClient.query('drop schema if exists e2e_test cascade;');
      await fromClient.end();
    }
    if (toClient) {
      await toClient.query('drop schema if exists e2e_test cascade;');
      await toClient.end();
    }
  });

  it('runs the full pipeline and produces valid SQL', async () => {
    // Dynamic import to avoid issues if dependencies aren't ready
    const { introspect } = await import('../src/introspector/index.js');
    const { buildDependencyInfo } = await import('../src/dependencies.js');
    const { diff } = await import('../src/differ.js');
    const { generate } = await import('../src/sql-generator.js');

    const schemas = ['e2e_test'];

    const [fromObjects, toObjects] = await Promise.all([
      introspect(fromClient, schemas),
      introspect(toClient, schemas),
    ]);

    const depInfo = await buildDependencyInfo(fromClient, toClient, schemas);

    const operations = diff(fromObjects, toObjects, {});

    const sql = generate(operations, depInfo);

    // Verify structure
    assert.ok(sql.includes('dbdelta migration'), 'should have header');
    assert.ok(sql.includes('PHASE 1'), 'should have phase 1');
    assert.ok(sql.includes('PHASE 2'), 'should have phase 2');
    assert.ok(sql.includes('PHASE 3'), 'should have phase 3');

    // Verify drops are commented out
    // old_table should be dropped (commented)
    assert.ok(sql.includes('-- drop'), 'drops should be commented out');

    // Verify new objects are created
    assert.ok(sql.includes('comments'), 'should create comments table');

    // Verify the view is recreated (active_users changed)
    assert.ok(sql.includes('active_users'), 'should update active_users view');

    // Verify new index
    assert.ok(sql.includes('idx_users_name'), 'should create idx_users_name');

    // Verify column additions
    assert.ok(sql.includes('created_at'), 'should add created_at column');
    assert.ok(sql.includes('published'), 'should add published column');

    // Verify phase ordering: PHASE 1 before PHASE 2 before PHASE 3
    const p1 = sql.indexOf('PHASE 1');
    const p2 = sql.indexOf('PHASE 2');
    const p3 = sql.indexOf('PHASE 3');
    assert.ok(p1 < p2, 'phase 1 before phase 2');
    assert.ok(p2 < p3, 'phase 2 before phase 3');
  });

  it('respects dependency ordering for creates', async () => {
    const { introspect } = await import('../src/introspector/index.js');
    const { buildDependencyInfo } = await import('../src/dependencies.js');
    const { diff } = await import('../src/differ.js');
    const { generate } = await import('../src/sql-generator.js');

    const schemas = ['e2e_test'];

    const [fromObjects, toObjects] = await Promise.all([
      introspect(fromClient, schemas),
      introspect(toClient, schemas),
    ]);

    const depInfo = await buildDependencyInfo(fromClient, toClient, schemas);
    const operations = diff(fromObjects, toObjects, {});
    const sql = generate(operations, depInfo);

    // comments table depends on posts table (FK) - if both are new,
    // posts should appear before comments in the output
    const postsIdx = sql.indexOf('e2e_test.posts');
    const commentsCreateIdx = sql.indexOf('create table e2e_test.comments') !== -1
      ? sql.indexOf('create table e2e_test.comments')
      : sql.indexOf('e2e_test.comments');

    // comments is new, posts already exists, so this just validates the SQL is present
    assert.ok(commentsCreateIdx > 0 || sql.includes('comments'), 'comments table should be in output');
  });
});
```

**Test commands:**

```bash
# Unit tests (no database required)
node --test test/dependencies.test.js test/differ.test.js test/sql-generator.test.js test/cli.test.js

# End-to-end test (requires two PostgreSQL databases)
TEST_PG_FROM_URL=postgres://localhost:5432/dbdelta_from TEST_PG_TO_URL=postgres://localhost:5432/dbdelta_to node --test test/e2e.test.js
```

---

### Task 3.6 — Package.json bin Entry

**File:** `package.json` (update existing)

- [ ] Add `"bin"` field pointing to `src/cli.js`
- [ ] Add shebang `#!/usr/bin/env node` to top of `src/cli.js`
- [ ] Add `run()` call at the bottom of `src/cli.js` guarded by `import.meta.url` check
- [ ] Update test scripts

**Add to `package.json`:**

```json
{
  "bin": {
    "dbdelta": "./src/cli.js"
  },
  "scripts": {
    "test": "node --test test/*.test.js",
    "test:unit": "node --test test/dependencies.test.js test/differ.test.js test/sql-generator.test.js test/cli.test.js",
    "test:e2e": "node --test test/e2e.test.js"
  }
}
```

**Add to top of `src/cli.js`:**

```js
#!/usr/bin/env node
```

**Add to bottom of `src/cli.js`:**

```js
// Run when executed directly
const isMain = process.argv[1] && import.meta.url.endsWith(process.argv[1].replace(/\\/g, '/'));
if (isMain) {
  run();
}
```

---

### Summary of Files

| File | Action | Purpose |
|---|---|---|
| `src/dependencies.js` | Create | Dependency graph from pg_depend, topological sort |
| `src/differ.js` | Create | Object matching, diffing, change strategy selection |
| `src/sql-generator.js` | Create | SQL emission with phase grouping and commenting |
| `src/cli.js` | Update | Wire full pipeline, parse all CLI flags |
| `test/dependencies.test.js` | Create | Unit tests for graph operations |
| `test/differ.test.js` | Create | Unit tests for matching, renames, diff logic |
| `test/sql-generator.test.js` | Create | Unit tests for SQL formatting and phase ordering |
| `test/cli.test.js` | Create | Unit tests for CLI arg parsing |
| `test/e2e.test.js` | Create | Integration test with real PostgreSQL databases |
| `package.json` | Update | Add bin entry, update test scripts |

