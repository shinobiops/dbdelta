import { identity } from './identity.js';

export async function introspectConstraints(client, schemas) {
  const result = await client.query(`
    select
      n.nspname as schema_name,
      c.relname as table_name,
      con.conname as constraint_name,
      con.contype as constraint_type,
      pg_catalog.pg_get_constraintdef(con.oid, true) as definition,
      con.condeferrable as deferrable,
      con.condeferred as deferred
    from pg_catalog.pg_constraint con
    join pg_catalog.pg_class c on c.oid = con.conrelid
    join pg_catalog.pg_namespace n on n.oid = c.relnamespace
    where n.nspname = any($1)
      and c.relkind in ('r', 'p')
    order by n.nspname, c.relname, con.conname
  `, [schemas]);

  return result.rows.map(row => {
    const qualifiedName = `${row.table_name}.${row.constraint_name}`;
    const typeLabel = {
      p: 'primary_key',
      f: 'foreign_key',
      u: 'unique',
      c: 'check',
      x: 'exclusion',
    }[row.constraint_type] || row.constraint_type;

    let ddlCreate = '';
    // Unique/PK constraints create a backing index; drop any existing index with
    // the same name first to avoid "relation already exists" errors.
    if (row.constraint_type === 'u' || row.constraint_type === 'p') {
      ddlCreate += `drop index if exists ${qualify(row.schema_name, row.constraint_name)};\n`;
    }
    ddlCreate += `alter table ${qualify(row.schema_name, row.table_name)} add constraint ${quote(row.constraint_name)} ${row.definition}`;
    if (row.deferrable) {
      ddlCreate += ' deferrable';
      if (row.deferred) {
        ddlCreate += ' initially deferred';
      }
    }
    ddlCreate += ';';

    return {
      identity: identity(row.schema_name, 'constraint', qualifiedName),
      definition: {
        name: row.constraint_name,
        table: row.table_name,
        schema: row.schema_name,
        constraint_type: typeLabel,
        definition: row.definition,
        deferrable: row.deferrable,
        deferred: row.deferred,
      },
      ddl: {
        create: ddlCreate,
        // drop: `alter table ${qualify(row.schema_name, row.table_name)} drop constraint ${quote(row.constraint_name)};`,
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
