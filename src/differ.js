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
  'operator', 'aggregate', 'cast', 'statistics', 'collation',
  'ts_config', 'ts_dictionary', 'ts_parser', 'ts_template',
  'opclass', 'rule', 'event_trigger',
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

  // Functions/procedures: CREATE OR REPLACE only if signature is compatible
  // PostgreSQL cannot change return type or parameters via CREATE OR REPLACE
  if (objectType === 'function' || objectType === 'procedure') {
    if (fromDef.result_type !== toDef.result_type || fromDef.identity_args !== toDef.identity_args) {
      return 'DROP_AND_CREATE';
    }
    return 'CREATE_OR_REPLACE';
  }

  // Views: prefer CREATE OR REPLACE
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
 * Takes two arrays of introspected objects and options.
 * Returns array of change operations.
 */
export function diff(fromObjects, toObjects, options = {}) {
  const { renames: renameArgs, excludeTypes, excludeSchemas, schemas } = options;
  const schemasSet = schemas && schemas.length > 0 ? new Set(schemas) : null;

  // Filter excluded types and schemas
  const filterObj = (obj) => {
    if (excludeTypes && excludeTypes.includes(obj.identity.type)) return false;
    if (excludeSchemas && excludeSchemas.includes(obj.identity.schema)) return false;
    if (schemasSet && !schemasSet.has(obj.identity.schema)) return false;
    return true;
  };

  const filteredFrom = fromObjects.filter(filterObj);
  const filteredTo = toObjects.filter(filterObj);

  const parsedRenames = parseRenames(renameArgs);
  const { matched, createOnly, dropOnly } = matchObjects(filteredFrom, filteredTo, parsedRenames);

  const ops = [];

  // Build set of tables/views being dropped so we can skip their children
  const droppedRelations = new Set();
  for (const obj of dropOnly) {
    const t = obj.identity.type;
    if (t === 'table' || t === 'view' || t === 'materialized_view' || t === 'foreign_table') {
      droppedRelations.add(`${obj.identity.schema}.${obj.identity.name}`);
    }
  }

  // DROP operations for removed objects
  for (const obj of dropOnly) {
    // Skip child objects whose parent table is already being dropped
    const parentTable = obj.definition?.table || obj.definition?.table_name;
    if (parentTable && droppedRelations.has(`${obj.identity.schema}.${parentTable}`)) {
      continue;
    }
    // Skip grants on objects being dropped
    if (obj.identity.type === 'grant' && obj.definition?.object_name &&
        droppedRelations.has(`${obj.definition.schema}.${obj.definition.object_name}`)) {
      continue;
    }

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
        reason: 'definition changed',
      });
    }
  }

  return ops;
}
