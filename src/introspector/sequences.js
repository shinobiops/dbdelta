import { identity } from './identity.js';

export async function introspectSequences(client, schemas) {
  const result = await client.query(`
    select
      n.nspname as schema_name,
      c.relname as sequence_name,
      pg_catalog.format_type(s.seqtypid, null) as data_type,
      s.seqstart as start_value,
      s.seqincrement as increment,
      s.seqmin as min_value,
      s.seqmax as max_value,
      s.seqcache as cache_size,
      s.seqcycle as cycle,
      pg_catalog.pg_get_userbyid(c.relowner) as owner,
      d.refobjid::regclass::text as owned_by_table,
      a.attname as owned_by_column
    from pg_catalog.pg_sequence s
    join pg_catalog.pg_class c on c.oid = s.seqrelid
    join pg_catalog.pg_namespace n on n.oid = c.relnamespace
    left join pg_catalog.pg_depend d on d.objid = c.oid
      and d.deptype = 'a'
      and d.classid = 'pg_class'::regclass
      and d.refclassid = 'pg_class'::regclass
    left join pg_catalog.pg_attribute a on a.attrelid = d.refobjid
      and a.attnum = d.refobjsubid
    where n.nspname = any($1)
    order by n.nspname, c.relname
  `, [schemas]);

  return result.rows.map(row => {
    let ddlCreate = `create sequence ${qualify(row.schema_name, row.sequence_name)}`;
    if (row.data_type !== 'bigint') {
      ddlCreate += ` as ${row.data_type}`;
    }
    ddlCreate += ` increment by ${row.increment}`;
    ddlCreate += ` minvalue ${row.min_value}`;
    ddlCreate += ` maxvalue ${row.max_value}`;
    ddlCreate += ` start with ${row.start_value}`;
    ddlCreate += ` cache ${row.cache_size}`;
    if (row.cycle) ddlCreate += ' cycle';
    else ddlCreate += ' no cycle';
    ddlCreate += ';';

    if (row.owned_by_table && row.owned_by_column) {
      ddlCreate += `\nalter sequence ${qualify(row.schema_name, row.sequence_name)} owned by ${row.owned_by_table}.${quote(row.owned_by_column)};`;
    }

    return {
      identity: identity(row.schema_name, 'sequence', row.sequence_name),
      definition: {
        name: row.sequence_name,
        schema: row.schema_name,
        data_type: row.data_type,
        start_value: row.start_value,
        increment: row.increment,
        min_value: row.min_value,
        max_value: row.max_value,
        cache_size: row.cache_size,
        cycle: row.cycle,
        owner: row.owner,
        owned_by_table: row.owned_by_table,
        owned_by_column: row.owned_by_column,
      },
      ddl: {
        create: ddlCreate,
        // drop: `drop sequence ${qualify(row.schema_name, row.sequence_name)};`,
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
