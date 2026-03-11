import { identity } from './identity.js';

export async function introspectPolicies(client, schemas) {
  const result = await client.query(`
    select
      n.nspname as schema_name,
      c.relname as table_name,
      pol.polname as policy_name,
      pol.polcmd as command,
      pol.polpermissive as permissive,
      pg_catalog.pg_get_expr(pol.polqual, pol.polrelid, true) as using_expr,
      pg_catalog.pg_get_expr(pol.polwithcheck, pol.polrelid, true) as with_check_expr,
      coalesce(
        (select array_agg(rolname::text order by rolname)
         from pg_catalog.pg_roles
         where oid = any(pol.polroles)),
        array[]::text[]
      ) as roles
    from pg_catalog.pg_policy pol
    join pg_catalog.pg_class c on c.oid = pol.polrelid
    join pg_catalog.pg_namespace n on n.oid = c.relnamespace
    where n.nspname = any($1)
    order by n.nspname, c.relname, pol.polname
  `, [schemas]);

  return result.rows.map(row => {
    const policyIdentityName = `${row.policy_name}.on.${row.table_name}`;
    const commandMap = { '*': 'all', r: 'select', a: 'insert', w: 'update', d: 'delete' };
    const command = commandMap[row.command] || row.command;
    const q = qualify(row.schema_name, row.table_name);
    const roles = row.roles.length > 0 ? row.roles.map(r => quote(r)).join(', ') : 'public';

    const createParts = [`create policy ${quote(row.policy_name)} on ${q}`];
    if (!row.permissive) {
      createParts.push('  as restrictive');
    }
    createParts.push(`  for ${command}`);
    createParts.push(`  to ${roles}`);
    if (row.using_expr) {
      createParts.push(`  using (${row.using_expr})`);
    }
    if (row.with_check_expr) {
      createParts.push(`  with check (${row.with_check_expr})`);
    }
    const createDdl = createParts.join('\n') + ';';

    return {
      identity: identity(row.schema_name, 'policy', policyIdentityName),
      definition: {
        name: row.policy_name,
        schema: row.schema_name,
        table_name: row.table_name,
        command,
        permissive: row.permissive,
        using_expr: row.using_expr || null,
        with_check_expr: row.with_check_expr || null,
        roles: row.roles.length > 0 ? row.roles : ['public'],
      },
      ddl: {
        create: createDdl,
        drop: `drop policy ${quote(row.policy_name)} on ${q};`,
        alter: (fromDef, toDef) => {
          const stmts = [];
          const target = `policy ${quote(toDef.name)} on ${qualify(toDef.schema, toDef.table_name)}`;
          if (fromDef.using_expr !== toDef.using_expr) {
            stmts.push(`alter ${target} using (${toDef.using_expr || 'true'});`);
          }
          if (fromDef.with_check_expr !== toDef.with_check_expr) {
            stmts.push(`alter ${target} with check (${toDef.with_check_expr || 'true'});`);
          }
          const fromRoles = (fromDef.roles || []).sort().join(',');
          const toRoles = (toDef.roles || []).sort().join(',');
          if (fromRoles !== toRoles) {
            const rolesList = toDef.roles.map(r => quote(r)).join(', ');
            stmts.push(`alter ${target} to ${rolesList};`);
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
