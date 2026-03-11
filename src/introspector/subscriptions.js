import { identity } from './identity.js';

export async function introspectSubscriptions(client, schemas) {
  const result = await client.query(`
    select
      s.subname as name,
      pg_catalog.pg_get_userbyid(s.subowner) as owner,
      s.subenabled as enabled,
      s.subslotname as slot_name,
      s.subpublications as publications,
      s.subsynccommit as sync_commit
    from pg_catalog.pg_subscription s
    order by s.subname
  `);

  return result.rows.map(row => {
    const pubList = (row.publications || []).map(p => quote(p)).join(', ');
    const optParts = [];
    if (row.slot_name) optParts.push(`slot_name = '${row.slot_name}'`);
    if (!row.enabled) optParts.push('enabled = false');
    const optClause = optParts.length > 0 ? ` with (${optParts.join(', ')})` : '';

    return {
      identity: identity('pg_global', 'subscription', row.name),
      definition: {
        name: row.name,
        owner: row.owner,
        enabled: row.enabled,
        slot_name: row.slot_name,
        publications: row.publications || [],
        sync_commit: row.sync_commit,
      },
      ddl: {
        create: `create subscription ${quote(row.name)} connection '<CONNINFO>' publication ${pubList}${optClause};`,
        drop: `drop subscription ${quote(row.name)};`,
      },
    };
  });
}

function quote(name) {
  if (/^[a-z_][a-z0-9_]*$/.test(name)) return name;
  return `"${name}"`;
}
