import { identity } from './identity.js';

export async function introspectUserMappings(client, schemas) {
  const result = await client.query(`
    select
      s.srvname as server_name,
      case um.umuser
        when 0 then 'public'
        else pg_catalog.pg_get_userbyid(um.umuser)
      end as user_name,
      (select array_agg(option_name || '=' || option_value order by option_name)
       from pg_catalog.pg_options_to_table(um.umoptions)) as options
    from pg_catalog.pg_user_mapping um
    join pg_catalog.pg_foreign_server s on s.oid = um.umserver
    order by s.srvname, user_name
  `);

  return result.rows.map(row => {
    const mappingName = `${row.server_name}/${row.user_name}`;
    const userClause = row.user_name === 'public' ? 'public' : quote(row.user_name);
    const optClause = row.options && row.options.length > 0
      ? ` options (${row.options.map(o => { const [k, v] = o.split('=', 2); return `${k} '${v}'`; }).join(', ')})`
      : '';

    return {
      identity: identity('pg_global', 'user_mapping', mappingName),
      definition: {
        server_name: row.server_name,
        user_name: row.user_name,
        options: row.options || [],
      },
      ddl: {
        create: `create user mapping for ${userClause} server ${quote(row.server_name)}${optClause};`,
        drop: `drop user mapping for ${userClause} server ${quote(row.server_name)};`,
      },
    };
  });
}

function quote(name) {
  if (/^[a-z_][a-z0-9_]*$/.test(name)) return name;
  return `"${name}"`;
}
