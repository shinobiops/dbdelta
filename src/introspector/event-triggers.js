import { identity } from './identity.js';

export async function introspectEventTriggers(client, schemas) {
  const result = await client.query(`
    select
      e.evtname as name,
      pg_catalog.pg_get_userbyid(e.evtowner) as owner,
      e.evtevent as event,
      e.evtenabled as enabled,
      e.evttags as tags,
      p.proname as function_name,
      n.nspname as function_schema
    from pg_catalog.pg_event_trigger e
    join pg_catalog.pg_proc p on p.oid = e.evtfoid
    join pg_catalog.pg_namespace n on n.oid = p.pronamespace
    order by e.evtname
  `);

  return result.rows.map(row => {
    const tagsClause = row.tags && row.tags.length > 0
      ? `\n  when tag in (${row.tags.map(t => `'${t}'`).join(', ')})`
      : '';
    const enabledMap = { O: 'enable', D: 'disable', R: 'enable replica', A: 'enable always' };
    const enabledState = enabledMap[row.enabled] || 'enable';

    return {
      identity: identity('pg_global', 'event_trigger', row.name),
      definition: {
        name: row.name,
        owner: row.owner,
        event: row.event,
        enabled: row.enabled,
        tags: row.tags || [],
        function_name: row.function_name,
        function_schema: row.function_schema,
      },
      ddl: {
        create: `create event trigger ${quote(row.name)} on ${row.event}${tagsClause}\n  execute function ${qualify(row.function_schema, row.function_name)}();`,
        drop: `drop event trigger ${quote(row.name)};`,
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
