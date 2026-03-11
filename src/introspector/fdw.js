import { identity } from './identity.js';

export async function introspectFdw(client, schemas) {
  const results = [];

  // Foreign Data Wrappers
  const fdwResult = await client.query(`
    select
      fdw.fdwname as name,
      pg_catalog.pg_get_userbyid(fdw.fdwowner) as owner,
      fdw.fdwhandler::regproc::text as handler,
      fdw.fdwvalidator::regproc::text as validator,
      (select array_agg(option_name || '=' || option_value order by option_name)
       from pg_catalog.pg_options_to_table(fdw.fdwoptions)) as options
    from pg_catalog.pg_foreign_data_wrapper fdw
    order by fdw.fdwname
  `);

  for (const row of fdwResult.rows) {
    const optClause = row.options && row.options.length > 0
      ? ` options (${row.options.map(o => { const [k, v] = o.split('=', 2); return `${k} '${v}'`; }).join(', ')})`
      : '';
    const handlerClause = row.handler && row.handler !== '-' ? ` handler ${row.handler}` : ' no handler';
    const validatorClause = row.validator && row.validator !== '-' ? ` validator ${row.validator}` : ' no validator';

    results.push({
      identity: identity('pg_global', 'fdw', row.name),
      definition: {
        name: row.name,
        owner: row.owner,
        handler: row.handler,
        validator: row.validator,
        options: row.options || [],
      },
      ddl: {
        create: `create foreign data wrapper ${quote(row.name)}${handlerClause}${validatorClause}${optClause};`,
        drop: `drop foreign data wrapper ${quote(row.name)};`,
        alter: (fromDef, toDef) => {
          const stmts = [];
          const target = `foreign data wrapper ${quote(toDef.name)}`;
          const fromOpts = (fromDef.options || []).sort().join(',');
          const toOpts = (toDef.options || []).sort().join(',');
          if (fromOpts !== toOpts) {
            const optParts = (toDef.options || []).map(o => {
              const [k, v] = o.split('=', 2);
              return `${k} '${v}'`;
            });
            if (optParts.length > 0) {
              stmts.push(`alter ${target} options (${optParts.map(p => `set ${p}`).join(', ')});`);
            }
          }
          return stmts.join('\n');
        },
      },
    });
  }

  // Foreign Servers
  const serverResult = await client.query(`
    select
      s.srvname as name,
      pg_catalog.pg_get_userbyid(s.srvowner) as owner,
      fdw.fdwname as fdw_name,
      s.srvtype as server_type,
      s.srvversion as server_version,
      (select array_agg(option_name || '=' || option_value order by option_name)
       from pg_catalog.pg_options_to_table(s.srvoptions)) as options
    from pg_catalog.pg_foreign_server s
    join pg_catalog.pg_foreign_data_wrapper fdw on fdw.oid = s.srvfdw
    order by s.srvname
  `);

  for (const row of serverResult.rows) {
    const optClause = row.options && row.options.length > 0
      ? ` options (${row.options.map(o => { const [k, v] = o.split('=', 2); return `${k} '${v}'`; }).join(', ')})`
      : '';
    const typeClause = row.server_type ? ` type '${row.server_type}'` : '';
    const versionClause = row.server_version ? ` version '${row.server_version}'` : '';

    results.push({
      identity: identity('pg_global', 'foreign_server', row.name),
      definition: {
        name: row.name,
        owner: row.owner,
        fdw_name: row.fdw_name,
        server_type: row.server_type,
        server_version: row.server_version,
        options: row.options || [],
      },
      ddl: {
        create: `create server ${quote(row.name)} foreign data wrapper ${quote(row.fdw_name)}${typeClause}${versionClause}${optClause};`,
        drop: `drop server ${quote(row.name)};`,
        alter: (fromDef, toDef) => {
          const stmts = [];
          const target = `server ${quote(toDef.name)}`;
          const fromOpts = (fromDef.options || []).sort().join(',');
          const toOpts = (toDef.options || []).sort().join(',');
          if (fromOpts !== toOpts) {
            const optParts = (toDef.options || []).map(o => {
              const [k, v] = o.split('=', 2);
              return `${k} '${v}'`;
            });
            if (optParts.length > 0) {
              stmts.push(`alter ${target} options (${optParts.map(p => `set ${p}`).join(', ')});`);
            }
          }
          return stmts.join('\n');
        },
      },
    });
  }

  // Foreign Tables
  const ftResult = await client.query(`
    select
      n.nspname as schema_name,
      c.relname as table_name,
      s.srvname as server_name,
      pg_catalog.pg_get_userbyid(c.relowner) as owner,
      (select array_agg(option_name || '=' || option_value order by option_name)
       from pg_catalog.pg_options_to_table(ft.ftoptions)) as options,
      (select array_agg(
        a.attname || ' ' || pg_catalog.format_type(a.atttypid, a.atttypmod)
        order by a.attnum)
       from pg_catalog.pg_attribute a
       where a.attrelid = c.oid and a.attnum > 0 and not a.attisdropped) as columns
    from pg_catalog.pg_foreign_table ft
    join pg_catalog.pg_class c on c.oid = ft.ftrelid
    join pg_catalog.pg_namespace n on n.oid = c.relnamespace
    join pg_catalog.pg_foreign_server s on s.oid = ft.ftserver
    where n.nspname = any($1)
    order by n.nspname, c.relname
  `, [schemas]);

  for (const row of ftResult.rows) {
    const cols = (row.columns || []).join(', ');
    const optClause = row.options && row.options.length > 0
      ? ` options (${row.options.map(o => { const [k, v] = o.split('=', 2); return `${k} '${v}'`; }).join(', ')})`
      : '';

    results.push({
      identity: identity(row.schema_name, 'foreign_table', row.table_name),
      definition: {
        name: row.table_name,
        schema: row.schema_name,
        server_name: row.server_name,
        owner: row.owner,
        columns: row.columns || [],
        options: row.options || [],
      },
      ddl: {
        create: `create foreign table ${qualify(row.schema_name, row.table_name)} (${cols}) server ${quote(row.server_name)}${optClause};`,
        drop: `drop foreign table ${qualify(row.schema_name, row.table_name)};`,
        alter: (fromDef, toDef) => {
          const stmts = [];
          const target = `foreign table ${qualify(toDef.schema, toDef.name)}`;
          const fromOpts = (fromDef.options || []).sort().join(',');
          const toOpts = (toDef.options || []).sort().join(',');
          if (fromOpts !== toOpts) {
            const optParts = (toDef.options || []).map(o => {
              const [k, v] = o.split('=', 2);
              return `${k} '${v}'`;
            });
            if (optParts.length > 0) {
              stmts.push(`alter ${target} options (${optParts.map(p => `set ${p}`).join(', ')});`);
            }
          }
          return stmts.join('\n');
        },
      },
    });
  }

  return results;
}

function quote(name) {
  if (/^[a-z_][a-z0-9_]*$/.test(name)) return name;
  return `"${name}"`;
}

function qualify(schema, name) {
  return `${quote(schema)}.${quote(name)}`;
}
