import { identity } from './identity.js';

export async function introspectPublications(client, schemas) {
  const result = await client.query(`
    select
      p.pubname as name,
      pg_catalog.pg_get_userbyid(p.pubowner) as owner,
      p.puballtables as all_tables,
      p.pubinsert as publish_insert,
      p.pubupdate as publish_update,
      p.pubdelete as publish_delete,
      p.pubtruncate as publish_truncate,
      (select array_agg(n.nspname || '.' || c.relname order by n.nspname, c.relname)
       from pg_catalog.pg_publication_rel pr
       join pg_catalog.pg_class c on c.oid = pr.prrelid
       join pg_catalog.pg_namespace n on n.oid = c.relnamespace
       where pr.prpubid = p.oid) as tables
    from pg_catalog.pg_publication p
    order by p.pubname
  `);

  return result.rows.map(row => {
    const publishParts = [];
    if (row.publish_insert) publishParts.push('insert');
    if (row.publish_update) publishParts.push('update');
    if (row.publish_delete) publishParts.push('delete');
    if (row.publish_truncate) publishParts.push('truncate');
    const publishClause = publishParts.length > 0 && publishParts.length < 4
      ? ` with (publish = '${publishParts.join(', ')}')`
      : '';

    let tableClause;
    if (row.all_tables) {
      tableClause = ' for all tables';
    } else if (row.tables && row.tables.length > 0) {
      tableClause = ` for table ${row.tables.join(', ')}`;
    } else {
      tableClause = '';
    }

    return {
      identity: identity('pg_global', 'publication', row.name),
      definition: {
        name: row.name,
        owner: row.owner,
        all_tables: row.all_tables,
        publish_insert: row.publish_insert,
        publish_update: row.publish_update,
        publish_delete: row.publish_delete,
        publish_truncate: row.publish_truncate,
        tables: row.tables || [],
      },
      ddl: {
        create: `create publication ${quote(row.name)}${tableClause}${publishClause};`,
        drop: `drop publication ${quote(row.name)};`,
      },
    };
  });
}

function quote(name) {
  if (/^[a-z_][a-z0-9_]*$/.test(name)) return name;
  return `"${name}"`;
}
