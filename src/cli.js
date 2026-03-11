#!/usr/bin/env node

export function parseArgs(argv) {
  const args = argv.slice(2);
  const result = {
    fromUrl: null,
    toUrl: null,
    schemas: ['public'],
    excludeSchemas: [],
    excludeTypes: [],
    renames: [],
  };

  const positional = [];
  let i = 0;
  while (i < args.length) {
    const arg = args[i];
    if (arg === '--schemas' && i + 1 < args.length) {
      result.schemas = args[++i].split(',').map(s => s.trim());
    } else if (arg === '--exclude-schemas' && i + 1 < args.length) {
      result.excludeSchemas = args[++i].split(',').map(s => s.trim());
    } else if (arg === '--exclude-types' && i + 1 < args.length) {
      result.excludeTypes = args[++i].split(',').map(s => s.trim());
    } else if (arg === '--rename' && i + 1 < args.length) {
      result.renames.push(parseRename(args[++i]));
    } else if (!arg.startsWith('--')) {
      positional.push(arg);
    } else {
      throw new Error(`Unknown option: ${arg}`);
    }
    i++;
  }

  if (positional.length < 2) {
    throw new Error('Usage: dbdelta <fromUrl> <toUrl> [options]');
  }
  result.fromUrl = positional[0];
  result.toUrl = positional[1];
  return result;
}

export function parseRename(spec) {
  const parts = spec.split(':');
  if (parts.length !== 3) {
    throw new Error(`Invalid rename spec: ${spec}`);
  }
  const [kind, from, to] = parts;
  if (kind === 'table') {
    return { kind: 'table', from, to };
  }
  if (kind === 'column') {
    const slashIdx = from.indexOf('/');
    if (slashIdx === -1) {
      throw new Error(`Column rename must use / to separate table from column: ${spec}`);
    }
    const tablePart = from.slice(0, slashIdx);
    const column = from.slice(slashIdx + 1);
    let schema = 'public';
    let table = tablePart;
    const dotIdx = tablePart.indexOf('.');
    if (dotIdx !== -1) {
      schema = tablePart.slice(0, dotIdx);
      table = tablePart.slice(dotIdx + 1);
    }
    return { kind: 'column', schema, table, from: column, to };
  }
  throw new Error(`Unknown rename kind: ${kind}`);
}

async function main() {
  try {
    const config = parseArgs(process.argv);
    console.error('dbdelta: parsed config', JSON.stringify(config, null, 2));
  } catch (err) {
    console.error(`Error: ${err.message}`);
    process.exit(1);
  }
}

const isMain = process.argv[1] && import.meta.url.endsWith(process.argv[1].replace(/\\/g, '/'));
if (isMain) {
  main();
}
