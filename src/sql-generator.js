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
      const sql = resolveDdl(op.ddl.drop) || fallbackDrop(identity);
      lines.push(commentOut(sql));
      break;
    }
    case 'CREATE': {
      lines.push(`-- create ${identity.type} ${label}`);
      const sql = resolveDdl(op.ddl.createOrReplace) || resolveDdl(op.ddl.create);
      lines.push(sql);
      break;
    }
    case 'ALTER': {
      lines.push(`-- alter ${identity.type} ${label}`);
      const sql = resolveDdl(op.ddl.alter, op.fromDef, op.toDef);
      if (sql) lines.push(sql);
      break;
    }
    case 'CREATE_OR_REPLACE': {
      lines.push(`-- create or replace ${identity.type} ${label}`);
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
      lines.push(`-- drop and recreate ${identity.type} ${label}`);
      const dropSql = resolveDdl(op.ddl.drop) || fallbackDrop(identity);
      lines.push(dropSql);
      const createSql = resolveDdl(op.ddl.create);
      lines.push(createSql);
      break;
    }
    case 'RENAME': {
      lines.push(`-- rename ${identity.type} ${label} -> ${op.newName}`);
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
