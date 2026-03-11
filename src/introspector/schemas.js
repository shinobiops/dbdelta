import { identity } from './identity.js';

const SYSTEM_SCHEMAS = ['pg_catalog', 'information_schema', 'pg_toast'];

export async function introspectSchemas(client, schemas) {
  const result = await client.query(`
    select
      n.nspname as schema_name,
      pg_catalog.pg_get_userbyid(n.nspowner) as owner
    from pg_catalog.pg_namespace n
    where n.nspname = any($1)
      and n.nspname not in ('pg_catalog', 'information_schema', 'pg_toast')
    order by n.nspname
  `, [schemas]);

  return result.rows.map(row => ({
    identity: identity(row.schema_name, 'schema', row.schema_name),
    definition: {
      name: row.schema_name,
      owner: row.owner,
    },
    ddl: {
      create: `create schema ${quote(row.schema_name)};`,
      drop: `drop schema ${quote(row.schema_name)};`,
    },
  }));
}

function quote(name) {
  if (/^[a-z_][a-z0-9_]*$/.test(name)) return name;
  return `"${name}"`;
}
