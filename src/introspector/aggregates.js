import { identity } from './identity.js';

export async function introspectAggregates(client, schemas) {
  const result = await client.query(`
    select
      n.nspname as schema_name,
      p.proname as agg_name,
      pg_catalog.pg_get_function_identity_arguments(p.oid) as identity_args,
      a.aggtransfn::regproc::text as state_func,
      pg_catalog.format_type(a.aggtranstype, null) as state_type,
      a.aggfinalfn::regproc::text as final_func,
      a.agginitval as init_val,
      a.aggsortop::regoperator::text as sort_operator,
      a.aggkind as kind,
      pg_catalog.pg_get_userbyid(p.proowner) as owner
    from pg_catalog.pg_aggregate a
    join pg_catalog.pg_proc p on p.oid = a.aggfnoid
    join pg_catalog.pg_namespace n on n.oid = p.pronamespace
    where n.nspname = any($1)
    order by n.nspname, p.proname, identity_args
  `, [schemas]);

  return result.rows.map(row => {
    const identityArgs = row.identity_args || '';
    const identityName = `${row.agg_name}(${identityArgs})`;
    const q = qualify(row.schema_name, row.agg_name);

    let ddlCreate = `create aggregate ${q}(${identityArgs}) (\n`;
    ddlCreate += `  sfunc = ${row.state_func},\n`;
    ddlCreate += `  stype = ${row.state_type}`;
    if (row.final_func && row.final_func !== '-') {
      ddlCreate += `,\n  finalfunc = ${row.final_func}`;
    }
    if (row.init_val !== null) {
      ddlCreate += `,\n  initcond = '${row.init_val}'`;
    }
    if (row.sort_operator && row.sort_operator !== '0') {
      ddlCreate += `,\n  sortop = ${row.sort_operator}`;
    }
    ddlCreate += '\n);';

    const ddlDrop = `drop aggregate if exists ${q}(${identityArgs});`;

    return {
      identity: identity(row.schema_name, 'aggregate', identityName),
      definition: {
        name: row.agg_name,
        schema: row.schema_name,
        identity_args: identityArgs,
        state_func: row.state_func,
        state_type: row.state_type,
        final_func: row.final_func !== '-' ? row.final_func : null,
        init_val: row.init_val,
        kind: row.kind,
        owner: row.owner,
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
