import { identity } from './identity.js';

export async function introspectOperators(client, schemas) {
  const result = await client.query(`
    select
      n.nspname as schema_name,
      o.oprname as operator_name,
      pg_catalog.format_type(o.oprleft, null) as left_type,
      pg_catalog.format_type(o.oprright, null) as right_type,
      pg_catalog.format_type(o.oprresult, null) as result_type,
      p.proname as function_name,
      pn.nspname as function_schema,
      com.oprname as commutator_name,
      com_ns.nspname as commutator_schema,
      neg.oprname as negator_name,
      neg_ns.nspname as negator_schema,
      o.oprcanhash as can_hash,
      o.oprcanmerge as can_merge
    from pg_catalog.pg_operator o
    join pg_catalog.pg_namespace n on n.oid = o.oprnamespace
    join pg_catalog.pg_proc p on p.oid = o.oprcode
    join pg_catalog.pg_namespace pn on pn.oid = p.pronamespace
    left join pg_catalog.pg_operator com on com.oid = o.oprcom
    left join pg_catalog.pg_namespace com_ns on com_ns.oid = com.oprnamespace
    left join pg_catalog.pg_operator neg on neg.oid = o.oprnegate
    left join pg_catalog.pg_namespace neg_ns on neg_ns.oid = neg.oprnamespace
    where n.nspname = any($1)
    order by n.nspname, o.oprname, left_type, right_type
  `, [schemas]);

  return result.rows.map(row => {
    const leftArg = row.left_type === '-' ? 'none' : row.left_type;
    const rightArg = row.right_type === '-' ? 'none' : row.right_type;
    const identityName = `${row.operator_name}(${leftArg},${rightArg})`;
    const q = qualify(row.schema_name, row.operator_name);

    let ddlCreate = `create operator ${q} (\n`;
    ddlCreate += `  function = ${qualify(row.function_schema, row.function_name)}`;
    if (row.left_type && row.left_type !== '-') {
      ddlCreate += `,\n  leftarg = ${row.left_type}`;
    }
    if (row.right_type && row.right_type !== '-') {
      ddlCreate += `,\n  rightarg = ${row.right_type}`;
    }
    if (row.commutator_name) {
      const comOp = row.commutator_schema === row.schema_name
        ? `operator(${row.commutator_name})`
        : `operator(${qualify(row.commutator_schema, row.commutator_name)})`;
      ddlCreate += `,\n  commutator = ${comOp}`;
    }
    if (row.negator_name) {
      const negOp = row.negator_schema === row.schema_name
        ? `operator(${row.negator_name})`
        : `operator(${qualify(row.negator_schema, row.negator_name)})`;
      ddlCreate += `,\n  negator = ${negOp}`;
    }
    if (row.can_hash) ddlCreate += `,\n  hashes`;
    if (row.can_merge) ddlCreate += `,\n  merges`;
    ddlCreate += `\n);`;

    let ddlDrop = `drop operator if exists ${q}(`;
    ddlDrop += row.left_type === '-' ? 'none' : row.left_type;
    ddlDrop += ', ';
    ddlDrop += row.right_type === '-' ? 'none' : row.right_type;
    ddlDrop += ');';

    return {
      identity: identity(row.schema_name, 'operator', identityName),
      definition: {
        name: row.operator_name,
        schema: row.schema_name,
        left_type: row.left_type === '-' ? null : row.left_type,
        right_type: row.right_type === '-' ? null : row.right_type,
        result_type: row.result_type,
        function_name: row.function_name,
        function_schema: row.function_schema,
        can_hash: row.can_hash,
        can_merge: row.can_merge,
      },
      ddl: {
        create: ddlCreate,
        drop: ddlDrop,
      },
    };
  });
}

function quote(name) {
  if (/^[a-z_][a-z0-9_]*$/.test(name)) return name;
  return `"${name}"`;
}

function qualify(schema, name) {
  return `${quote(schema)}.${quote(name)}`;
}
