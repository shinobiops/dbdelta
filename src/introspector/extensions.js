import { identity } from './identity.js';

export async function introspectExtensions(client, schemas) {
  const result = await client.query(`
    select
      e.extname as name,
      e.extversion as version,
      n.nspname as schema_name,
      e.extrelocatable as relocatable
    from pg_catalog.pg_extension e
    join pg_catalog.pg_namespace n on n.oid = e.extnamespace
    where n.nspname = any($1)
    order by e.extname
  `, [schemas]);

  return result.rows.map(row => ({
    identity: identity(row.schema_name, 'extension', row.name),
    definition: {
      name: row.name,
      version: row.version,
      schema: row.schema_name,
      relocatable: row.relocatable,
    },
    ddl: {
      create: `create extension if not exists "${row.name}" schema ${quote(row.schema_name)} version '${row.version}';`,
      drop: `drop extension if exists "${row.name}";`,
    },
  }));
}

function quote(name) {
  if (/^[a-z_][a-z0-9_]*$/.test(name)) return name;
  return `"${name}"`;
}
