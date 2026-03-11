import { identity } from './identity.js';

export async function introspectStatistics(client, schemas) {
  const result = await client.query(`
    select
      sn.nspname as schema_name,
      s.stxname as stat_name,
      s.stxkind::text[] as kinds,
      array_agg(a.attname::text order by a.attnum) as columns,
      c.relname as table_name,
      cn.nspname as table_schema,
      pg_catalog.pg_get_userbyid(s.stxowner) as owner
    from pg_catalog.pg_statistic_ext s
    join pg_catalog.pg_namespace sn on sn.oid = s.stxnamespace
    join pg_catalog.pg_class c on c.oid = s.stxrelid
    join pg_catalog.pg_namespace cn on cn.oid = c.relnamespace
    join pg_catalog.pg_attribute a on a.attrelid = s.stxrelid and a.attnum = any(s.stxkeys)
    where sn.nspname = any($1)
    group by sn.nspname, s.stxname, s.stxkind, c.relname, cn.nspname, s.stxowner
    order by sn.nspname, s.stxname
  `, [schemas]);

  const kindMap = { d: 'ndistinct', f: 'dependencies', m: 'mcv' };

  return result.rows.map(row => {
    const kinds = row.kinds.map(k => kindMap[k] || k);
    const q = qualify(row.schema_name, row.stat_name);
    const tableQ = qualify(row.table_schema, row.table_name);
    const cols = row.columns.map(c => quote(c)).join(', ');

    const ddlCreate = `create statistics ${q} (${kinds.join(', ')}) on ${cols} from ${tableQ};`;
    const ddlDrop = `drop statistics if exists ${q};`;

    return {
      identity: identity(row.schema_name, 'statistics', row.stat_name),
      definition: {
        name: row.stat_name,
        schema: row.schema_name,
        kinds,
        columns: row.columns,
        table_name: row.table_name,
        table_schema: row.table_schema,
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
