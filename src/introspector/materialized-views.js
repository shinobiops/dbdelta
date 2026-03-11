import { identity } from './identity.js';

export async function introspectMaterializedViews(client, schemas) {
  const result = await client.query(`
    select
      n.nspname as schema_name,
      c.relname as matview_name,
      pg_catalog.pg_get_viewdef(c.oid, true) as view_def,
      pg_catalog.pg_get_userbyid(c.relowner) as owner,
      t.spcname as tablespace
    from pg_catalog.pg_class c
    join pg_catalog.pg_namespace n on n.oid = c.relnamespace
    left join pg_catalog.pg_tablespace t on t.oid = c.reltablespace
    where n.nspname = any($1)
      and c.relkind = 'm'
    order by n.nspname, c.relname
  `, [schemas]);

  return result.rows.map(row => {
    const q = qualify(row.schema_name, row.matview_name);
    const viewDef = row.view_def.trim().replace(/;$/, '');

    let ddlCreate = `create materialized view ${q} as\n${viewDef}\nwith no data;`;

    return {
      identity: identity(row.schema_name, 'materialized_view', row.matview_name),
      definition: {
        name: row.matview_name,
        schema: row.schema_name,
        view_definition: viewDef,
        owner: row.owner,
        tablespace: row.tablespace,
      },
      ddl: {
        create: ddlCreate,
        alter: (fromDef, toDef) => {
          const stmts = [];
          const target = qualify(toDef.schema, toDef.name);
          if (fromDef.owner !== toDef.owner) {
            stmts.push(`alter materialized view ${target} owner to ${quote(toDef.owner)};`);
          }
          return stmts.join('\n');
        },
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
