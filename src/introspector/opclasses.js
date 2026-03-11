import { identity } from './identity.js';

export async function introspectOpClasses(client, schemas) {
  const result = await client.query(`
    select
      n.nspname as schema_name,
      oc.opcname as opclass_name,
      am.amname as access_method,
      pg_catalog.format_type(oc.opcintype, null) as input_type,
      oc.opcdefault as is_default,
      of.opfname as family_name,
      fn.nspname as family_schema,
      pg_catalog.pg_get_userbyid(oc.opcowner) as owner
    from pg_catalog.pg_opclass oc
    join pg_catalog.pg_namespace n on n.oid = oc.opcnamespace
    join pg_catalog.pg_am am on am.oid = oc.opcmethod
    join pg_catalog.pg_opfamily of on of.oid = oc.opcfamily
    join pg_catalog.pg_namespace fn on fn.oid = of.opfnamespace
    where n.nspname = any($1)
    order by n.nspname, oc.opcname
  `, [schemas]);

  return result.rows.map(row => {
    let ddlCreate = `create operator class ${qualify(row.schema_name, row.opclass_name)}`;
    if (row.is_default) ddlCreate += ' default';
    ddlCreate += ` for type ${row.input_type} using ${row.access_method}`;
    ddlCreate += ` family ${qualify(row.family_schema, row.family_name)}`;
    ddlCreate += ` as storage ${row.input_type};`;

    const ddlDrop = `drop operator class if exists ${qualify(row.schema_name, row.opclass_name)} using ${row.access_method};`;

    return {
      identity: identity(row.schema_name, 'opclass', row.opclass_name),
      definition: {
        name: row.opclass_name,
        schema: row.schema_name,
        access_method: row.access_method,
        input_type: row.input_type,
        is_default: row.is_default,
        family_name: row.family_name,
        family_schema: row.family_schema,
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
