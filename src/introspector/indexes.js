import { identity } from './identity.js';

export async function introspectIndexes(client, schemas) {
  const result = await client.query(`
    select
      n.nspname as schema_name,
      ci.relname as index_name,
      ct.relname as table_name,
      am.amname as access_method,
      i.indisunique as is_unique,
      pg_catalog.pg_get_indexdef(i.indexrelid) as indexdef,
      pg_catalog.pg_get_expr(i.indpred, i.indrelid) as predicate
    from pg_catalog.pg_index i
    join pg_catalog.pg_class ci on ci.oid = i.indexrelid
    join pg_catalog.pg_class ct on ct.oid = i.indrelid
    join pg_catalog.pg_namespace n on n.oid = ci.relnamespace
    join pg_catalog.pg_am am on am.oid = ci.relam
    where n.nspname = any($1)
      and ct.relkind in ('r', 'p')
      and not i.indisprimary
      and not exists (
        select 1 from pg_catalog.pg_constraint c
        where c.conindid = i.indexrelid
      )
    order by n.nspname, ct.relname, ci.relname
  `, [schemas]);

  return result.rows.map(row => ({
    identity: identity(row.schema_name, 'index', row.index_name),
    definition: {
      name: row.index_name,
      table: row.table_name,
      schema: row.schema_name,
      access_method: row.access_method,
      is_unique: row.is_unique,
      indexdef: row.indexdef,
      predicate: row.predicate,
    },
    ddl: {
      create: `${row.indexdef};`,
      // drop: `drop index ${qualify(row.schema_name, row.index_name)};`,
    },
  }));
}

function quote(name) {
  if (/^[a-z_][a-z0-9_]*$/.test(name)) return name;
  return `"${name}"`;
}

function qualify(schema, name) {
  return `${quote(schema)}.${quote(name)}`;
}
