import { identity } from './identity.js';

export async function introspectCasts(client, schemas) {
  const result = await client.query(`
    select
      pg_catalog.format_type(c.castsource, null) as source_type,
      pg_catalog.format_type(c.casttarget, null) as target_type,
      c.castfunc::regproc::text as cast_func,
      c.castcontext as cast_context,
      c.castmethod as cast_method,
      sn.nspname as source_schema,
      tn.nspname as target_schema
    from pg_catalog.pg_cast c
    join pg_catalog.pg_type st on st.oid = c.castsource
    join pg_catalog.pg_namespace sn on sn.oid = st.typnamespace
    join pg_catalog.pg_type tt on tt.oid = c.casttarget
    join pg_catalog.pg_namespace tn on tn.oid = tt.typnamespace
    where (sn.nspname = any($1) or tn.nspname = any($1))
    order by source_type, target_type
  `, [schemas]);

  const contextMap = { e: 'explicit', a: 'assignment', i: 'implicit' };

  return result.rows.map(row => {
    const identityName = `${row.source_type}::${row.target_type}`;
    const schema = row.source_schema !== 'pg_catalog' ? row.source_schema : row.target_schema;

    let ddlCreate = `create cast (${row.source_type} as ${row.target_type})`;
    if (row.cast_method === 'f') {
      ddlCreate += ` with function ${row.cast_func}`;
    } else if (row.cast_method === 'i') {
      ddlCreate += ' with inout';
    } else {
      ddlCreate += ' without function';
    }
    if (row.cast_context === 'a') {
      ddlCreate += ' as assignment';
    } else if (row.cast_context === 'i') {
      ddlCreate += ' as implicit';
    }
    ddlCreate += ';';

    const ddlDrop = `drop cast if exists (${row.source_type} as ${row.target_type});`;

    return {
      identity: identity(schema, 'cast', identityName),
      definition: {
        source_type: row.source_type,
        target_type: row.target_type,
        cast_func: row.cast_func !== '-' ? row.cast_func : null,
        cast_context: contextMap[row.cast_context] || row.cast_context,
        cast_method: row.cast_method,
      },
      ddl: {
        create: ddlCreate,
        drop: ddlDrop,
      },
    };
  });
}
