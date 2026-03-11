import { identity } from './identity.js';

export async function introspectTextSearch(client, schemas) {
  const configs = await introspectTsConfigs(client, schemas);
  const dicts = await introspectTsDictionaries(client, schemas);
  const parsers = await introspectTsParsers(client, schemas);
  const templates = await introspectTsTemplates(client, schemas);
  return [...configs, ...dicts, ...parsers, ...templates];
}

async function introspectTsConfigs(client, schemas) {
  const result = await client.query(`
    select
      n.nspname as schema_name,
      c.cfgname as config_name,
      pn.nspname as parser_schema,
      p.prsname as parser_name,
      pg_catalog.pg_get_userbyid(c.cfgowner) as owner
    from pg_catalog.pg_ts_config c
    join pg_catalog.pg_namespace n on n.oid = c.cfgnamespace
    join pg_catalog.pg_ts_parser p on p.oid = c.cfgparser
    join pg_catalog.pg_namespace pn on pn.oid = p.prsnamespace
    where n.nspname = any($1)
    order by n.nspname, c.cfgname
  `, [schemas]);

  return result.rows.map(row => {
    const q = qualify(row.schema_name, row.config_name);
    const parser = qualify(row.parser_schema, row.parser_name);
    const ddlCreate = `create text search configuration ${q} (parser = ${parser});`;
    const ddlDrop = `drop text search configuration if exists ${q};`;

    return {
      identity: identity(row.schema_name, 'ts_config', row.config_name),
      definition: {
        name: row.config_name,
        schema: row.schema_name,
        parser_name: row.parser_name,
        parser_schema: row.parser_schema,
        owner: row.owner,
      },
      ddl: {
        create: ddlCreate,
        drop: ddlDrop,
      },
    };
  });
}

async function introspectTsDictionaries(client, schemas) {
  const result = await client.query(`
    select
      n.nspname as schema_name,
      d.dictname as dict_name,
      tn.nspname as template_schema,
      t.tmplname as template_name,
      d.dictinitoption as options,
      pg_catalog.pg_get_userbyid(d.dictowner) as owner
    from pg_catalog.pg_ts_dict d
    join pg_catalog.pg_namespace n on n.oid = d.dictnamespace
    join pg_catalog.pg_ts_template t on t.oid = d.dicttemplate
    join pg_catalog.pg_namespace tn on tn.oid = t.tmplnamespace
    where n.nspname = any($1)
    order by n.nspname, d.dictname
  `, [schemas]);

  return result.rows.map(row => {
    const q = qualify(row.schema_name, row.dict_name);
    const template = qualify(row.template_schema, row.template_name);
    let ddlCreate = `create text search dictionary ${q} (template = ${template}`;
    if (row.options) ddlCreate += `, ${row.options}`;
    ddlCreate += ');';
    const ddlDrop = `drop text search dictionary if exists ${q};`;

    return {
      identity: identity(row.schema_name, 'ts_dictionary', row.dict_name),
      definition: {
        name: row.dict_name,
        schema: row.schema_name,
        template_name: row.template_name,
        template_schema: row.template_schema,
        options: row.options,
        owner: row.owner,
      },
      ddl: {
        create: ddlCreate,
        drop: ddlDrop,
      },
    };
  });
}

async function introspectTsParsers(client, schemas) {
  const result = await client.query(`
    select
      n.nspname as schema_name,
      p.prsname as parser_name,
      p.prsstart::regproc::text as start_func,
      p.prstoken::regproc::text as gettoken_func,
      p.prsend::regproc::text as end_func,
      p.prslextype::regproc::text as lextypes_func,
      p.prsheadline::regproc::text as headline_func
    from pg_catalog.pg_ts_parser p
    join pg_catalog.pg_namespace n on n.oid = p.prsnamespace
    where n.nspname = any($1)
    order by n.nspname, p.prsname
  `, [schemas]);

  return result.rows.map(row => {
    const q = qualify(row.schema_name, row.parser_name);
    let ddlCreate = `-- note: requires C functions\n`;
    ddlCreate += `create text search parser ${q} (\n`;
    ddlCreate += `  start = ${row.start_func},\n`;
    ddlCreate += `  gettoken = ${row.gettoken_func},\n`;
    ddlCreate += `  end = ${row.end_func},\n`;
    ddlCreate += `  lextypes = ${row.lextypes_func}`;
    if (row.headline_func && row.headline_func !== '-') {
      ddlCreate += `,\n  headline = ${row.headline_func}`;
    }
    ddlCreate += '\n);';
    const ddlDrop = `drop text search parser if exists ${q};`;

    return {
      identity: identity(row.schema_name, 'ts_parser', row.parser_name),
      definition: {
        name: row.parser_name,
        schema: row.schema_name,
        start_func: row.start_func,
        gettoken_func: row.gettoken_func,
        end_func: row.end_func,
        lextypes_func: row.lextypes_func,
        headline_func: row.headline_func !== '-' ? row.headline_func : null,
      },
      ddl: {
        create: ddlCreate,
        drop: ddlDrop,
      },
    };
  });
}

async function introspectTsTemplates(client, schemas) {
  const result = await client.query(`
    select
      n.nspname as schema_name,
      t.tmplname as template_name,
      t.tmplinit::regproc::text as init_func,
      t.tmpllexize::regproc::text as lexize_func
    from pg_catalog.pg_ts_template t
    join pg_catalog.pg_namespace n on n.oid = t.tmplnamespace
    where n.nspname = any($1)
    order by n.nspname, t.tmplname
  `, [schemas]);

  return result.rows.map(row => {
    const q = qualify(row.schema_name, row.template_name);
    let ddlCreate = `-- note: requires C functions\n`;
    ddlCreate += `create text search template ${q} (\n`;
    if (row.init_func && row.init_func !== '-') {
      ddlCreate += `  init = ${row.init_func},\n`;
    }
    ddlCreate += `  lexize = ${row.lexize_func}\n);`;
    const ddlDrop = `drop text search template if exists ${q};`;

    return {
      identity: identity(row.schema_name, 'ts_template', row.template_name),
      definition: {
        name: row.template_name,
        schema: row.schema_name,
        init_func: row.init_func !== '-' ? row.init_func : null,
        lexize_func: row.lexize_func,
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
