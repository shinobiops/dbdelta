import { identity } from './identity.js';

export async function introspectTriggers(client, schemas) {
  const result = await client.query(`
    select
      n.nspname as schema_name,
      t.tgname as trigger_name,
      c.relname as table_name,
      pg_catalog.pg_get_triggerdef(t.oid, true) as trigger_def,
      p.proname as function_name,
      pn.nspname as function_schema
    from pg_catalog.pg_trigger t
    join pg_catalog.pg_class c on c.oid = t.tgrelid
    join pg_catalog.pg_namespace n on n.oid = c.relnamespace
    join pg_catalog.pg_proc p on p.oid = t.tgfoid
    join pg_catalog.pg_namespace pn on pn.oid = p.pronamespace
    where n.nspname = any($1)
      and not t.tgisinternal
    order by n.nspname, c.relname, t.tgname
  `, [schemas]);

  return result.rows.map(row => {
    const triggerIdentityName = `${row.trigger_name}.on.${row.table_name}`;
    const triggerDef = row.trigger_def.endsWith(';') ? row.trigger_def : row.trigger_def + ';';

    return {
      identity: identity(row.schema_name, 'trigger', triggerIdentityName),
      definition: {
        name: row.trigger_name,
        schema: row.schema_name,
        table_name: row.table_name,
        function_name: row.function_name,
        function_schema: row.function_schema,
        trigger_definition: triggerDef,
      },
      ddl: {
        create: triggerDef,
        drop: `drop trigger ${quote(row.trigger_name)} on ${qualify(row.schema_name, row.table_name)};`,
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
