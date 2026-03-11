import { identity } from './identity.js';

export async function introspectCollations(client, schemas) {
  const result = await client.query(`
    select
      n.nspname as schema_name,
      c.collname as collation_name,
      c.collprovider as provider,
      c.collcollate as lc_collate,
      c.collctype as lc_ctype,
      c.collisdeterministic as deterministic,
      pg_catalog.pg_get_userbyid(c.collowner) as owner
    from pg_catalog.pg_collation c
    join pg_catalog.pg_namespace n on n.oid = c.collnamespace
    where n.nspname = any($1)
      and c.collencoding in (-1, (select encoding from pg_catalog.pg_database where datname = current_database()))
    order by n.nspname, c.collname
  `, [schemas]);

  const providerMap = { c: 'libc', i: 'icu', d: 'default' };

  return result.rows.map(row => {
    const q = qualify(row.schema_name, row.collation_name);
    const provider = providerMap[row.provider] || row.provider;

    let ddlCreate = `create collation ${q} (`;
    ddlCreate += `provider = ${provider}`;
    if (row.lc_collate) ddlCreate += `, locale = '${row.lc_collate}'`;
    if (row.lc_ctype && row.lc_ctype !== row.lc_collate) {
      ddlCreate += `, lc_ctype = '${row.lc_ctype}'`;
    }
    if (!row.deterministic) ddlCreate += ', deterministic = false';
    ddlCreate += ');';

    const ddlDrop = `drop collation if exists ${q};`;

    return {
      identity: identity(row.schema_name, 'collation', row.collation_name),
      definition: {
        name: row.collation_name,
        schema: row.schema_name,
        provider,
        lc_collate: row.lc_collate,
        lc_ctype: row.lc_ctype,
        deterministic: row.deterministic,
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
