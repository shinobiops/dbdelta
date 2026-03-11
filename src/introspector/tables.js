import { identity } from './identity.js';

export async function introspectTables(client, schemas) {
  const result = await client.query(`
    select
      n.nspname as schema_name,
      c.relname as table_name,
      pg_catalog.pg_get_userbyid(c.relowner) as owner,
      case c.relpersistence
        when 'p' then 'permanent'
        when 'u' then 'unlogged'
        when 't' then 'temporary'
      end as persistence,
      c.reloptions as options,
      ts.spcname as tablespace,
      c.relkind as relkind,
      case when c.relkind = 'p' then pg_catalog.pg_get_partkeydef(c.oid) else null end as partition_key,
      case when c.relispartition then pg_catalog.pg_get_expr(c.relpartbound, c.oid) else null end as partition_bound
    from pg_catalog.pg_class c
    join pg_catalog.pg_namespace n on n.oid = c.relnamespace
    left join pg_catalog.pg_tablespace ts on ts.oid = c.reltablespace
    where n.nspname = any($1)
      and c.relkind in ('r', 'p')
    order by n.nspname, c.relname
  `, [schemas]);

  return result.rows.map(row => {
    let ddlCreate = `create`;
    if (row.persistence === 'unlogged') ddlCreate += ' unlogged';
    ddlCreate += ` table ${qualify(row.schema_name, row.table_name)} ()`;
    if (row.partition_key) {
      ddlCreate += ` partition by ${row.partition_key}`;
    }
    if (row.partition_bound) {
      ddlCreate += ` ${row.partition_bound}`;
    }
    if (row.tablespace) {
      ddlCreate += ` tablespace ${quote(row.tablespace)}`;
    }
    ddlCreate += ';';

    return {
      identity: identity(row.schema_name, 'table', row.table_name),
      definition: {
        name: row.table_name,
        schema: row.schema_name,
        owner: row.owner,
        persistence: row.persistence,
        options: row.options,
        tablespace: row.tablespace,
        relkind: row.relkind,
        partition_key: row.partition_key,
        partition_bound: row.partition_bound,
      },
      ddl: {
        create: ddlCreate,
        // drop: `drop table ${qualify(row.schema_name, row.table_name)};`,
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
