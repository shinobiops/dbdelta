import { identity } from './identity.js';

export async function introspectRoles(client, schemas) {
  const result = await client.query(`
    select
      r.rolname as name,
      r.rolsuper as superuser,
      r.rolinherit as inherit,
      r.rolcreaterole as createrole,
      r.rolcreatedb as createdb,
      r.rolcanlogin as login,
      r.rolreplication as replication,
      r.rolbypassrls as bypassrls,
      r.rolconnlimit as connection_limit,
      r.rolvaliduntil as valid_until,
      (select array_agg(m.rolname order by m.rolname)
       from pg_catalog.pg_auth_members am
       join pg_catalog.pg_roles m on m.oid = am.roleid
       where am.member = r.oid) as member_of
    from pg_catalog.pg_roles r
    where r.rolname not like 'pg_%'
    order by r.rolname
  `);

  return result.rows.map(row => {
    const opts = [];
    if (row.superuser) opts.push('superuser');
    if (!row.inherit) opts.push('noinherit');
    if (row.createrole) opts.push('createrole');
    if (row.createdb) opts.push('createdb');
    if (row.login) opts.push('login');
    if (row.replication) opts.push('replication');
    if (row.bypassrls) opts.push('bypassrls');
    if (row.connection_limit >= 0) opts.push(`connection limit ${row.connection_limit}`);
    if (row.valid_until) opts.push(`valid until '${row.valid_until}'`);
    const optsClause = opts.length > 0 ? ` ${opts.join(' ')}` : '';

    return {
      identity: identity('pg_global', 'role', row.name),
      definition: {
        name: row.name,
        superuser: row.superuser,
        inherit: row.inherit,
        createrole: row.createrole,
        createdb: row.createdb,
        login: row.login,
        replication: row.replication,
        bypassrls: row.bypassrls,
        connection_limit: row.connection_limit,
        valid_until: row.valid_until,
        member_of: row.member_of || [],
      },
      ddl: {
        create: `create role ${quote(row.name)}${optsClause};`,
        drop: `drop role ${quote(row.name)};`,
      },
    };
  });
}

function quote(name) {
  if (/^[a-z_][a-z0-9_]*$/.test(name)) return name;
  return `"${name}"`;
}
