import { identity } from './identity.js';

export async function introspectRules(client, schemas) {
  const result = await client.query(`
    select
      n.nspname as schema_name,
      r.rulename as rule_name,
      c.relname as table_name,
      pg_catalog.pg_get_ruledef(r.oid, true) as rule_def,
      r.ev_type as event_type,
      r.ev_enabled as enabled,
      r.is_instead as is_instead
    from pg_catalog.pg_rewrite r
    join pg_catalog.pg_class c on c.oid = r.ev_class
    join pg_catalog.pg_namespace n on n.oid = c.relnamespace
    where n.nspname = any($1)
      and r.rulename <> '_RETURN'
    order by n.nspname, c.relname, r.rulename
  `, [schemas]);

  return result.rows.map(row => {
    const ruleIdentityName = `${row.rule_name}.on.${row.table_name}`;
    const ruleDef = row.rule_def.endsWith(';') ? row.rule_def : row.rule_def + ';';
    const eventMap = { 1: 'select', 2: 'update', 3: 'insert', 4: 'delete' };

    return {
      identity: identity(row.schema_name, 'rule', ruleIdentityName),
      definition: {
        name: row.rule_name,
        schema: row.schema_name,
        table_name: row.table_name,
        event_type: eventMap[row.event_type] || row.event_type,
        is_instead: row.is_instead,
        enabled: row.enabled,
        rule_definition: ruleDef,
      },
      ddl: {
        create: ruleDef,
        drop: `drop rule ${quote(row.rule_name)} on ${qualify(row.schema_name, row.table_name)};`,
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
