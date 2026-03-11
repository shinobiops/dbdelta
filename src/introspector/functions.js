import { identity } from './identity.js';

export async function introspectFunctions(client, schemas) {
  const result = await client.query(`
    select
      n.nspname as schema_name,
      p.proname as function_name,
      pg_catalog.pg_get_function_identity_arguments(p.oid) as identity_args,
      pg_catalog.pg_get_function_result(p.oid) as result_type,
      pg_catalog.pg_get_functiondef(p.oid) as function_def,
      l.lanname as language,
      p.prokind as kind,
      p.provolatile as volatility,
      p.proisstrict as strict,
      p.prosecdef as security_definer,
      p.procost as cost,
      p.prorows as rows,
      p.proparallel as parallel,
      pg_catalog.pg_get_userbyid(p.proowner) as owner
    from pg_catalog.pg_proc p
    join pg_catalog.pg_namespace n on n.oid = p.pronamespace
    join pg_catalog.pg_language l on l.oid = p.prolang
    where n.nspname = any($1)
      and p.prokind <> 'a'
    order by n.nspname, p.proname, identity_args
  `, [schemas]);

  return result.rows.map(row => {
    const identityArgs = row.identity_args || '';
    const nameWithArgs = `${row.function_name}(${identityArgs})`;
    const q = qualify(row.schema_name, row.function_name);
    const volatilityMap = { i: 'immutable', s: 'stable', v: 'volatile' };
    const parallelMap = { s: 'safe', r: 'restricted', u: 'unsafe' };
    const kindLabel = row.kind === 'p' ? 'procedure' : 'function';

    return {
      identity: identity(row.schema_name, 'function', nameWithArgs),
      definition: {
        name: row.function_name,
        schema: row.schema_name,
        identity_args: identityArgs,
        result_type: row.result_type,
        language: row.language,
        kind: row.kind,
        volatility: volatilityMap[row.volatility] || row.volatility,
        strict: row.strict,
        security_definer: row.security_definer,
        cost: parseFloat(row.cost),
        rows: parseFloat(row.rows),
        parallel: parallelMap[row.parallel] || row.parallel,
        owner: row.owner,
      },
      ddl: {
        drop: `drop ${kindLabel} ${q}(${identityArgs});`,
        createOrReplace: row.function_def.endsWith(';') ? row.function_def : row.function_def + ';',
        alter: (fromDef, toDef) => {
          const stmts = [];
          const target = `${kindLabel} ${qualify(toDef.schema, toDef.name)}(${toDef.identity_args})`;
          if (fromDef.owner !== toDef.owner) {
            stmts.push(`alter ${target} owner to ${quote(toDef.owner)};`);
          }
          if (fromDef.volatility !== toDef.volatility) {
            stmts.push(`alter ${target} ${toDef.volatility};`);
          }
          if (fromDef.security_definer !== toDef.security_definer) {
            stmts.push(`alter ${target} ${toDef.security_definer ? 'security definer' : 'security invoker'};`);
          }
          if (fromDef.cost !== toDef.cost) {
            stmts.push(`alter ${target} cost ${toDef.cost};`);
          }
          return stmts.join('\n');
        },
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
