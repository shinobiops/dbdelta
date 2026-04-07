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
 * Resolve a DDL field that may be a string or a function.
 */
function resolveDdl(ddlValue, ...args) {
  if (typeof ddlValue === 'function') return ddlValue(...args);
  return ddlValue || '';
}

/**
 * Generate a drop SQL statement for an object when ddl.drop is not available.
 * Constructs a reasonable default based on type and identity.
 */
function fallbackDrop(identity) {
  const q = `${quoteIdent(identity.schema)}.${quoteIdent(identity.name)}`;
  const typeMap = {
    table: `drop table ${q};`,
    view: `drop view ${q};`,
    materialized_view: `drop materialized view ${q};`,
    index: `drop index ${q};`,
    sequence: `drop sequence ${q};`,
    function: `drop function ${q};`,
    procedure: `drop procedure ${q};`,
    type: `drop type ${q};`,
    enum: `drop type ${q};`,
    composite_type: `drop type ${q};`,
    domain: `drop domain ${q};`,
    range: `drop type ${q};`,
    schema: `drop schema ${quoteIdent(identity.name)};`,
    extension: `drop extension if exists "${identity.name}";`,
    column: (() => {
      // Column names are like "table.column"
      const parts = identity.name.split('.');
      if (parts.length === 2) {
        return `alter table ${quoteIdent(identity.schema)}.${quoteIdent(parts[0])} drop column ${quoteIdent(parts[1])};`;
      }
      return `-- drop column ${q};`;
    })(),
    constraint: (() => {
      const parts = identity.name.split('.');
      if (parts.length === 2) {
        return `alter table ${quoteIdent(identity.schema)}.${quoteIdent(parts[0])} drop constraint ${quoteIdent(parts[1])};`;
      }
      return `-- drop constraint ${q};`;
    })(),
  };
  return typeMap[identity.type] || `-- drop ${identity.type} ${q};`;
}

function quoteIdent(name) {
  if (/^[a-z_][a-z0-9_]*$/.test(name)) return name;
  return `"${name}"`;
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

const TYPE_KINDS = new Set(['enum', 'composite_type', 'domain', 'range']);
const FUNCTION_KINDS = new Set(['function', 'procedure']);

/**
 * Find columns in fromObjects that depend on a given type.
 * Matches by schema-qualified type name in the column's data_type.
 */
function findDependentColumns(fromObjects, schema, typeName) {
  const qualifiedName = `${schema}.${typeName}`;
  return fromObjects.filter(obj =>
    obj.identity.type === 'column' &&
    obj.definition.data_type &&
    (obj.definition.data_type === typeName ||
     obj.definition.data_type === qualifiedName ||
     obj.definition.data_type === `${quoteIdent(schema)}.${quoteIdent(typeName)}`)
  );
}

/**
 * Check if an object has dependents using the dependency graph.
 * Handles key format mismatches between introspector identities and dep graph keys.
 */
function hasDependents(identity, fromGraph) {
  if (!fromGraph || fromGraph.size === 0) return false;

  // The dep graph uses keys like schema.type.name where:
  // - enums are 'type' not 'enum'
  // - functions use just proname (no args)
  let depKey;
  if (TYPE_KINDS.has(identity.type)) {
    depKey = `${identity.schema}.type.${identity.name}`;
  } else if (FUNCTION_KINDS.has(identity.type)) {
    const baseName = identity.name.includes('(')
      ? identity.name.substring(0, identity.name.indexOf('('))
      : identity.name;
    depKey = `${identity.schema}.function.${baseName}`;
  } else {
    depKey = identityKey(identity);
  }

  // Check reverse edges: anything that depends on this key
  for (const [, deps] of fromGraph) {
    if (deps.has(depKey)) return true;
  }
  return false;
}

/**
 * Generate safe swap SQL for a type DROP_AND_CREATE.
 * Pattern: create temp → alter dependent columns → drop old → rename temp
 */
function emitTypeSafeSwap(op, fromObjects) {
  const { identity } = op;
  const schema = identity.schema;
  const name = identity.name;
  const tempName = `__dbdelta_new_${name}`;
  const qSchema = quoteIdent(schema);
  const qName = quoteIdent(name);
  const qTemp = quoteIdent(tempName);
  const lines = [];

  lines.push(`-- safe swap: ${qSchema}.${qName}`);

  // Step 1: Create type with temp name
  const createSql = resolveDdl(op.ddl.create);
  const tempCreateSql = createSql.replace(
    new RegExp(`${escapeRegex(qSchema)}\\.${escapeRegex(qName)}`, 'g'),
    `${qSchema}.${qTemp}`
  );
  lines.push(tempCreateSql);

  // Step 2: Alter dependent columns to use temp type
  const depCols = findDependentColumns(fromObjects, schema, name);
  for (const col of depCols) {
    const tbl = `${qSchema}.${quoteIdent(col.definition.table)}`;
    const colName = quoteIdent(col.definition.name);
    lines.push(`alter table ${tbl} alter column ${colName} set data type ${qSchema}.${qTemp} using ${colName}::text::${qSchema}.${qTemp};`);
  }

  // Step 3: Drop old type
  const dropSql = resolveDdl(op.ddl.drop) || fallbackDrop(identity);
  lines.push(dropSql);

  // Step 4: Rename temp to original
  lines.push(`alter type ${qSchema}.${qTemp} rename to ${qName};`);

  return lines.join('\n');
}

/**
 * Generate safe swap SQL for a function/procedure DROP_AND_CREATE.
 * Pattern: rename old → create new → drop old renamed
 */
function emitFunctionSafeSwap(op) {
  const { identity } = op;
  const schema = identity.schema;
  const name = identity.name; // e.g. "get_foo(uuid)"
  const baseName = name.includes('(') ? name.substring(0, name.indexOf('(')) : name;
  const args = name.includes('(') ? name.substring(name.indexOf('(')) : '()';
  const tempBaseName = `__dbdelta_old_${baseName}`;
  const qSchema = quoteIdent(schema);
  const kindLabel = identity.type === 'procedure' ? 'procedure' : 'function';
  const lines = [];

  lines.push(`-- safe swap: ${qSchema}.${quoteIdent(baseName)}${args}`);

  // Step 1: Rename old function to temp name
  lines.push(`alter ${kindLabel} ${qSchema}.${quoteIdent(baseName)}${args} rename to ${quoteIdent(tempBaseName)};`);

  // Step 2: Create new function with original name
  const createSql = resolveDdl(op.ddl.createOrReplace) || resolveDdl(op.ddl.create);
  lines.push(createSql);

  // Step 3: Drop old renamed function
  lines.push(`drop ${kindLabel} ${qSchema}.${quoteIdent(tempBaseName)}${args};`);

  return lines.join('\n');
}

/**
 * Generate SQL for a simple DROP_AND_CREATE (no dependents).
 */
function emitSimpleDropAndCreate(op) {
  const { identity } = op;
  const lines = [];
  const dropSql = resolveDdl(op.ddl.drop) || fallbackDrop(identity);
  lines.push(dropSql);
  const createSql = resolveDdl(op.ddl.createOrReplace) || resolveDdl(op.ddl.create);
  lines.push(createSql);
  return lines.join('\n');
}

function escapeRegex(str) {
  return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

/**
 * Generate SQL for a single operation.
 */
function emitOp(op, fromObjects, fromGraph) {
  const { identity } = op;
  const lines = [];

  switch (op.op) {
    case 'DROP': {
      const sql = resolveDdl(op.ddl.drop) || fallbackDrop(identity);
      lines.push(commentOut(sql));
      break;
    }
    case 'CREATE': {
      const sql = resolveDdl(op.ddl.createOrReplace) || resolveDdl(op.ddl.create);
      lines.push(sql);
      break;
    }
    case 'ALTER': {
      const sql = resolveDdl(op.ddl.alter, op.fromDef, op.toDef);
      if (sql) lines.push(sql);
      break;
    }
    case 'CREATE_OR_REPLACE': {
      const sql = resolveDdl(op.ddl.createOrReplace) || resolveDdl(op.ddl.create);
      if (sql) lines.push(sql);
      // Also emit alter statements if the alter function exists and produces output
      if (op.ddl.alter && op.fromDef && op.toDef) {
        const alterSql = resolveDdl(op.ddl.alter, op.fromDef, op.toDef);
        if (alterSql) lines.push(alterSql);
      }
      break;
    }
    case 'DROP_AND_CREATE': {
      if (!hasDependents(identity, fromGraph)) {
        lines.push(emitSimpleDropAndCreate(op));
      } else if (TYPE_KINDS.has(identity.type)) {
        lines.push(emitTypeSafeSwap(op, fromObjects || []));
      } else if (FUNCTION_KINDS.has(identity.type)) {
        lines.push(emitFunctionSafeSwap(op));
      } else {
        lines.push(emitSimpleDropAndCreate(op));
      }
      break;
    }
    case 'RENAME': {
      if (op.ddl.rename) {
        const sql = resolveDdl(op.ddl.rename, identity.name, op.newName);
        lines.push(sql);
      } else {
        // Generate a default rename statement
        const q = `${quoteIdent(identity.schema)}.${quoteIdent(identity.name)}`;
        lines.push(`alter ${identity.type} ${q} rename to ${quoteIdent(op.newName)};`);
      }
      break;
    }
  }

  return lines.filter(l => l !== '').join('\n');
}

// All privileges by object type, used to detect when we can emit "all"
const ALL_PRIVILEGES = {
  table: ['DELETE', 'INSERT', 'MAINTAIN', 'REFERENCES', 'SELECT', 'TRIGGER', 'TRUNCATE', 'UPDATE'],
  view: ['DELETE', 'INSERT', 'REFERENCES', 'SELECT', 'TRIGGER', 'TRUNCATE', 'UPDATE'],
  sequence: ['SELECT', 'UPDATE', 'USAGE'],
  'foreign table': ['SELECT'],
  schema: ['CREATE', 'USAGE'],
  'materialized view': ['DELETE', 'INSERT', 'REFERENCES', 'SELECT', 'TRIGGER', 'TRUNCATE', 'UPDATE'],
};

/**
 * Group grant/revoke operations by action+object+grantee and emit combined SQL.
 */
function emitCombinedGrants(grantOps) {
  // Group by: op + object_type + schema.object_name + grantee + is_grantable
  const groups = new Map();
  for (const op of grantOps) {
    const def = op.toDef || op.fromDef || op.definition;
    if (!def) {
      // Fall back to emitting the raw DDL
      groups.set(identityKey(op.identity), { ops: [op], raw: true });
      continue;
    }
    const action = op.op === 'DROP' ? 'revoke' : 'grant';
    const key = `${action}|${def.object_type}|${def.schema}.${def.object_name}|${def.grantee}|${def.is_grantable || false}`;
    if (!groups.has(key)) {
      groups.set(key, { action, def, privileges: [] });
    }
    groups.get(key).privileges.push(def.privilege_type);
  }

  const lines = [];
  for (const group of groups.values()) {
    if (group.raw) {
      for (const op of group.ops) {
        const sql = resolveDdl(op.op === 'DROP' ? op.ddl.drop : op.ddl.create);
        lines.push(op.op === 'DROP' ? commentOut(sql) : sql);
      }
      continue;
    }

    const { action, def, privileges } = group;
    const allForType = ALL_PRIVILEGES[def.object_type];
    const sorted = [...privileges].sort();
    const privList = (allForType && sorted.length >= allForType.length && allForType.every(p => sorted.includes(p)))
      ? 'all'
      : sorted.join(', ');

    const grantType = (def.object_type === 'view' || def.object_type === 'materialized view')
      ? 'table' : def.object_type;
    const objectRef = grantType === 'schema'
      ? `schema ${quoteIdent(def.object_name)}`
      : `${grantType} ${quoteIdent(def.schema)}.${quoteIdent(def.object_name)}`;
    const grantOption = (action === 'grant' && def.is_grantable) ? ' with grant option' : '';
    const preposition = action === 'grant' ? 'to' : 'from';
    const role = def.grantee === 'public' ? 'public' : quoteIdent(def.grantee);

    const sql = `${action} ${privList} on ${objectRef} ${preposition} ${role}${grantOption};`;
    if (action === 'revoke') {
      lines.push(commentOut(sql));
    } else {
      lines.push(sql);
    }
  }
  return lines.join('\n');
}

/**
 * Main SQL generation entry point.
 * Takes ordered change operations, dependency info, and source objects.
 * @param {Array} operations - change operations from differ
 * @param {Object} dependencyInfo - { sortedCreates, sortedDrops }
 * @param {Array} [fromObjects] - introspected objects from source DB (for finding dependents)
 */
export function generate(operations, dependencyInfo, fromObjects) {
  const { sortedCreates, sortedDrops, fromGraph } = dependencyInfo;

  // Separate operations into phases
  const grantTypes = new Set(['grant', 'default_privilege']);
  const dropOps = operations.filter(o => o.op === 'DROP');
  const grantOps = operations.filter(o => grantTypes.has(o.identity.type));
  const createAlterOps = operations.filter(o =>
    o.op !== 'DROP' && !grantTypes.has(o.identity.type)
  );

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
      sections.push(emitOp(op, fromObjects, fromGraph));
    }
  }
  sections.push('');

  // Phase 2: Creates & Alters
  sections.push('-- === PHASE 2: CREATES & ALTERS ===');
  if (orderedCreateAlters.length > 0) {
    for (const op of orderedCreateAlters) {
      sections.push(emitOp(op, fromObjects, fromGraph));
    }
  }
  sections.push('');

  // Phase 3: Grants/Revokes (combined by object+grantee)
  sections.push('-- === PHASE 3: GRANTS/REVOKES ===');
  if (orderedGrants.length > 0) {
    sections.push(emitCombinedGrants(orderedGrants));
  }
  sections.push('');

  return sections.join('\n');
}
