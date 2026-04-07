import { identity } from './identity.js';

export async function introspectGrants(client, schemas) {
  const results = [];

  // Grants on tables/views/sequences (pg_class)
  const classResult = await client.query(`
    select
      n.nspname as schema_name,
      c.relname as object_name,
      case c.relkind
        when 'r' then 'table'
        when 'v' then 'view'
        when 'm' then 'materialized view'
        when 'S' then 'sequence'
        when 'f' then 'foreign table'
        else 'table'
      end as object_type,
      (aclexplode(c.relacl)).grantor::regrole::text as grantor,
      (aclexplode(c.relacl)).grantee::regrole::text as grantee,
      (aclexplode(c.relacl)).privilege_type as privilege_type,
      (aclexplode(c.relacl)).is_grantable as is_grantable
    from pg_catalog.pg_class c
    join pg_catalog.pg_namespace n on n.oid = c.relnamespace
    where n.nspname = any($1)
      and c.relacl is not null
    order by n.nspname, c.relname, grantee, privilege_type
  `, [schemas]);

  for (const row of classResult.rows) {
    const grantee = row.grantee === '-' ? 'public' : row.grantee;
    const grantName = `${row.privilege_type}/${row.schema_name}.${row.object_name}/${grantee}`;
    const grantOption = row.is_grantable ? ' with grant option' : '';
    const qualifiedObj = qualify(row.schema_name, row.object_name);
    const grantType = (row.object_type === 'view' || row.object_type === 'materialized view')
      ? 'table' : row.object_type;

    results.push({
      identity: identity(row.schema_name, 'grant', grantName),
      definition: {
        privilege_type: row.privilege_type,
        object_type: row.object_type,
        schema: row.schema_name,
        object_name: row.object_name,
        grantor: row.grantor,
        grantee,
        is_grantable: row.is_grantable,
      },
      ddl: {
        create: `grant ${row.privilege_type} on ${grantType} ${qualifiedObj} to ${quoteRole(grantee)}${grantOption};`,
        drop: `revoke ${row.privilege_type} on ${grantType} ${qualifiedObj} from ${quoteRole(grantee)};`,
      },
    });
  }

  // Grants on schemas (pg_namespace)
  const nsResult = await client.query(`
    select
      n.nspname as schema_name,
      (aclexplode(n.nspacl)).grantor::regrole::text as grantor,
      (aclexplode(n.nspacl)).grantee::regrole::text as grantee,
      (aclexplode(n.nspacl)).privilege_type as privilege_type,
      (aclexplode(n.nspacl)).is_grantable as is_grantable
    from pg_catalog.pg_namespace n
    where n.nspname = any($1)
      and n.nspacl is not null
    order by n.nspname, grantee, privilege_type
  `, [schemas]);

  for (const row of nsResult.rows) {
    const grantee = row.grantee === '-' ? 'public' : row.grantee;
    const grantName = `${row.privilege_type}/schema.${row.schema_name}/${grantee}`;
    const grantOption = row.is_grantable ? ' with grant option' : '';

    results.push({
      identity: identity(row.schema_name, 'grant', grantName),
      definition: {
        privilege_type: row.privilege_type,
        object_type: 'schema',
        schema: row.schema_name,
        object_name: row.schema_name,
        grantor: row.grantor,
        grantee,
        is_grantable: row.is_grantable,
      },
      ddl: {
        create: `grant ${row.privilege_type} on schema ${quote(row.schema_name)} to ${quoteRole(grantee)}${grantOption};`,
        drop: `revoke ${row.privilege_type} on schema ${quote(row.schema_name)} from ${quoteRole(grantee)};`,
      },
    });
  }

  // Default Privileges (pg_default_acl)
  const defResult = await client.query(`
    select
      n.nspname as schema_name,
      pg_catalog.pg_get_userbyid(d.defaclrole) as owner,
      case d.defaclobjtype
        when 'r' then 'tables'
        when 'S' then 'sequences'
        when 'f' then 'functions'
        when 'T' then 'types'
        when 'n' then 'schemas'
        else d.defaclobjtype::text
      end as object_type,
      (aclexplode(d.defaclacl)).grantee::regrole::text as grantee,
      (aclexplode(d.defaclacl)).privilege_type as privilege_type,
      (aclexplode(d.defaclacl)).is_grantable as is_grantable
    from pg_catalog.pg_default_acl d
    left join pg_catalog.pg_namespace n on n.oid = d.defaclnamespace
    where (n.nspname = any($1) or d.defaclnamespace = 0)
    order by n.nspname, object_type, grantee, privilege_type
  `, [schemas]);

  for (const row of defResult.rows) {
    const grantee = row.grantee === '-' ? 'public' : row.grantee;
    const schema = row.schema_name || 'pg_global';
    const defName = `${row.privilege_type}/${row.object_type}/${row.owner}/${grantee}`;
    const grantOption = row.is_grantable ? ' with grant option' : '';
    const inSchemaClause = row.schema_name ? ` in schema ${quote(row.schema_name)}` : '';

    results.push({
      identity: identity(schema, 'default_privilege', defName),
      definition: {
        privilege_type: row.privilege_type,
        object_type: row.object_type,
        schema: row.schema_name,
        owner: row.owner,
        grantee,
        is_grantable: row.is_grantable,
      },
      ddl: {
        create: `alter default privileges for role ${quoteRole(row.owner)}${inSchemaClause} grant ${row.privilege_type} on ${row.object_type} to ${quoteRole(grantee)}${grantOption};`,
        drop: `alter default privileges for role ${quoteRole(row.owner)}${inSchemaClause} revoke ${row.privilege_type} on ${row.object_type} from ${quoteRole(grantee)};`,
      },
    });
  }

  return results;
}

function quote(name) {
  if (/^[a-z_][a-z0-9_]*$/.test(name)) return name;
  return `"${name}"`;
}

function quoteRole(name) {
  if (name === 'public') return 'public';
  return quote(name);
}

function qualify(schema, name) {
  return `${quote(schema)}.${quote(name)}`;
}
