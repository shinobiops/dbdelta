import { identity } from './identity.js';

export async function introspectTypes(client, schemas) {
  const enums = await introspectEnums(client, schemas);
  const composites = await introspectComposites(client, schemas);
  const domains = await introspectDomains(client, schemas);
  const ranges = await introspectRanges(client, schemas);
  return [...enums, ...composites, ...domains, ...ranges];
}

async function introspectEnums(client, schemas) {
  const result = await client.query(`
    select
      n.nspname as schema_name,
      t.typname as type_name,
      array_agg(e.enumlabel::text order by e.enumsortorder) as labels
    from pg_catalog.pg_type t
    join pg_catalog.pg_namespace n on n.oid = t.typnamespace
    join pg_catalog.pg_enum e on e.enumtypid = t.oid
    where n.nspname = any($1)
      and t.typtype = 'e'
    group by n.nspname, t.typname
    order by n.nspname, t.typname
  `, [schemas]);

  return result.rows.map(row => ({
    identity: identity(row.schema_name, 'type_enum', row.type_name),
    definition: {
      name: row.type_name,
      schema: row.schema_name,
      labels: row.labels,
    },
    ddl: {
      create: `create type ${qualify(row.schema_name, row.type_name)} as enum (${row.labels.map(l => `'${l}'`).join(', ')});`,
      // drop: `drop type ${qualify(row.schema_name, row.type_name)};`,
      alter: (fromDef, toDef) => {
        const stmts = [];
        for (const label of toDef.labels) {
          if (!fromDef.labels.includes(label)) {
            const idx = toDef.labels.indexOf(label);
            if (idx === 0) {
              stmts.push(`alter type ${qualify(toDef.schema, toDef.name)} add value '${label}' before '${toDef.labels[1]}';`);
            } else {
              stmts.push(`alter type ${qualify(toDef.schema, toDef.name)} add value '${label}' after '${toDef.labels[idx - 1]}';`);
            }
          }
        }
        return stmts.join('\n');
      },
    },
  }));
}

async function introspectComposites(client, schemas) {
  const result = await client.query(`
    select
      n.nspname as schema_name,
      t.typname as type_name,
      array_agg(a.attname::text order by a.attnum) as attr_names,
      array_agg(pg_catalog.format_type(a.atttypid, a.atttypmod)::text order by a.attnum) as attr_types
    from pg_catalog.pg_type t
    join pg_catalog.pg_namespace n on n.oid = t.typnamespace
    join pg_catalog.pg_attribute a on a.attrelid = t.typrelid
    where n.nspname = any($1)
      and t.typtype = 'c'
      and a.attnum > 0
      and not a.attisdropped
      and not exists (
        select 1 from pg_catalog.pg_class c
        where c.oid = t.typrelid and c.relkind in ('r', 'v', 'm', 'p', 'f')
      )
    group by n.nspname, t.typname
    order by n.nspname, t.typname
  `, [schemas]);

  return result.rows.map(row => {
    const attrs = row.attr_names.map((name, i) => ({
      name,
      type: row.attr_types[i],
    }));
    return {
      identity: identity(row.schema_name, 'type_composite', row.type_name),
      definition: {
        name: row.type_name,
        schema: row.schema_name,
        attributes: attrs,
      },
      ddl: {
        create: `create type ${qualify(row.schema_name, row.type_name)} as (${attrs.map(a => `${quote(a.name)} ${a.type}`).join(', ')});`,
        // drop: `drop type ${qualify(row.schema_name, row.type_name)};`,
        alter: (fromDef, toDef) => {
          const stmts = [];
          const fromNames = fromDef.attributes.map(a => a.name);
          const toNames = toDef.attributes.map(a => a.name);
          const q = qualify(toDef.schema, toDef.name);
          for (const attr of toDef.attributes) {
            if (!fromNames.includes(attr.name)) {
              stmts.push(`alter type ${q} add attribute ${quote(attr.name)} ${attr.type};`);
            } else {
              const fromAttr = fromDef.attributes.find(a => a.name === attr.name);
              if (fromAttr.type !== attr.type) {
                stmts.push(`alter type ${q} alter attribute ${quote(attr.name)} set data type ${attr.type};`);
              }
            }
          }
          for (const attr of fromDef.attributes) {
            if (!toNames.includes(attr.name)) {
              stmts.push(`alter type ${q} drop attribute ${quote(attr.name)};`);
            }
          }
          return stmts.join('\n');
        },
      },
    };
  });
}

async function introspectDomains(client, schemas) {
  const result = await client.query(`
    select
      n.nspname as schema_name,
      t.typname as type_name,
      pg_catalog.format_type(t.typbasetype, t.typtypmod) as base_type,
      t.typnotnull as not_null,
      t.typdefault as "default",
      coalesce(
        (select array_agg(pg_catalog.pg_get_constraintdef(c.oid)::text order by c.conname)
         from pg_catalog.pg_constraint c
         where c.contypid = t.oid),
        array[]::text[]
      ) as constraints,
      coalesce(
        (select array_agg(c.conname::text order by c.conname)
         from pg_catalog.pg_constraint c
         where c.contypid = t.oid),
        array[]::text[]
      ) as constraint_names
    from pg_catalog.pg_type t
    join pg_catalog.pg_namespace n on n.oid = t.typnamespace
    where n.nspname = any($1)
      and t.typtype = 'd'
    order by n.nspname, t.typname
  `, [schemas]);

  return result.rows.map(row => {
    const constraintDefs = row.constraint_names.map((name, i) => ({
      name,
      check: row.constraints[i],
    }));
    let ddlCreate = `create domain ${qualify(row.schema_name, row.type_name)} as ${row.base_type}`;
    if (row.not_null) ddlCreate += ' not null';
    if (row.default) ddlCreate += ` default ${row.default}`;
    for (const c of constraintDefs) {
      ddlCreate += ` constraint ${quote(c.name)} ${c.check}`;
    }
    ddlCreate += ';';

    return {
      identity: identity(row.schema_name, 'type_domain', row.type_name),
      definition: {
        name: row.type_name,
        schema: row.schema_name,
        base_type: row.base_type,
        not_null: row.not_null,
        default: row.default,
        constraints: constraintDefs,
      },
      ddl: {
        create: ddlCreate,
        // drop: `drop domain ${qualify(row.schema_name, row.type_name)};`,
      },
    };
  });
}

async function introspectRanges(client, schemas) {
  const result = await client.query(`
    select
      n.nspname as schema_name,
      t.typname as type_name,
      pg_catalog.format_type(r.rngsubtype, null) as subtype,
      r.rngcollation::regcollation::text as collation,
      r.rngcanonical::regproc::text as canonical,
      r.rngsubdiff::regproc::text as subtype_diff
    from pg_catalog.pg_type t
    join pg_catalog.pg_namespace n on n.oid = t.typnamespace
    join pg_catalog.pg_range r on r.rngtypid = t.oid
    where n.nspname = any($1)
      and t.typtype = 'r'
    order by n.nspname, t.typname
  `, [schemas]);

  return result.rows.map(row => {
    let ddlCreate = `create type ${qualify(row.schema_name, row.type_name)} as range (subtype = ${row.subtype}`;
    if (row.collation && row.collation !== '-') {
      ddlCreate += `, collation = ${row.collation}`;
    }
    if (row.canonical && row.canonical !== '-') {
      ddlCreate += `, canonical = ${row.canonical}`;
    }
    if (row.subtype_diff && row.subtype_diff !== '-') {
      ddlCreate += `, subtype_diff = ${row.subtype_diff}`;
    }
    ddlCreate += ');';

    return {
      identity: identity(row.schema_name, 'type_range', row.type_name),
      definition: {
        name: row.type_name,
        schema: row.schema_name,
        subtype: row.subtype,
        collation: row.collation !== '-' ? row.collation : null,
        canonical: row.canonical !== '-' ? row.canonical : null,
        subtype_diff: row.subtype_diff !== '-' ? row.subtype_diff : null,
      },
      ddl: {
        create: ddlCreate,
        // drop: `drop type ${qualify(row.schema_name, row.type_name)};`,
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
