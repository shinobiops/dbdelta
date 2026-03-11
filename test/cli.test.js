import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { parseArgs, parseRename } from '../src/cli.js';

describe('parseArgs', () => {
  it('parses two positional urls with defaults', () => {
    const result = parseArgs(['node', 'cli.js', 'postgres://localhost/from', 'postgres://localhost/to']);
    assert.equal(result.fromUrl, 'postgres://localhost/from');
    assert.equal(result.toUrl, 'postgres://localhost/to');
    assert.deepEqual(result.schemas, ['public']);
  });

  it('parses --schemas flag', () => {
    const result = parseArgs(['node', 'cli.js', 'from', 'to', '--schemas', 'public,app']);
    assert.deepEqual(result.schemas, ['public', 'app']);
  });

  it('parses --exclude-schemas flag', () => {
    const result = parseArgs(['node', 'cli.js', 'from', 'to', '--exclude-schemas', 'audit,logs']);
    assert.ok(result.excludeSchemas.includes('audit'));
    assert.ok(result.excludeSchemas.includes('logs'));
    // System schemas always included
    assert.ok(result.excludeSchemas.includes('pg_catalog'));
    assert.ok(result.excludeSchemas.includes('information_schema'));
  });

  it('parses --exclude-types flag', () => {
    const result = parseArgs(['node', 'cli.js', 'from', 'to', '--exclude-types', 'grants,roles']);
    assert.deepEqual(result.excludeTypes, ['grants', 'roles']);
  });

  it('parses multiple --rename flags as raw strings', () => {
    const result = parseArgs([
      'node', 'cli.js', 'from', 'to',
      '--rename', 'table:users:accounts',
      '--rename', 'column:app.orders/qty:quantity',
    ]);
    assert.equal(result.renames.length, 2);
    assert.equal(result.renames[0], 'table:users:accounts');
    assert.equal(result.renames[1], 'column:app.orders/qty:quantity');
  });

  it('throws on missing positional args', () => {
    assert.throws(() => parseArgs(['node', 'cli.js']), /Usage/);
  });

  it('throws on unknown option', () => {
    assert.throws(() => parseArgs(['node', 'cli.js', 'from', 'to', '--bogus']), /Unknown option/);
  });

  it('defaults excludeSchemas to system schemas', () => {
    const result = parseArgs(['node', 'cli.js', 'from', 'to']);
    assert.ok(result.excludeSchemas.includes('pg_catalog'));
    assert.ok(result.excludeSchemas.includes('information_schema'));
    assert.ok(result.excludeSchemas.includes('pg_toast'));
  });

  it('defaults excludeTypes to empty', () => {
    const result = parseArgs(['node', 'cli.js', 'from', 'to']);
    assert.deepEqual(result.excludeTypes, []);
  });

  it('defaults renames to empty array', () => {
    const result = parseArgs(['node', 'cli.js', 'from', 'to']);
    assert.deepEqual(result.renames, []);
  });
});

describe('parseRename', () => {
  it('parses table rename', () => {
    assert.deepEqual(parseRename('table:old:new'), { kind: 'table', from: 'old', to: 'new' });
  });

  it('parses column rename with schema', () => {
    assert.deepEqual(parseRename('column:myschema.mytable/old_col:new_col'), {
      kind: 'column', schema: 'myschema', table: 'mytable', from: 'old_col', to: 'new_col',
    });
  });

  it('parses column rename defaulting to public schema', () => {
    assert.deepEqual(parseRename('column:mytable/old_col:new_col'), {
      kind: 'column', schema: 'public', table: 'mytable', from: 'old_col', to: 'new_col',
    });
  });

  it('throws on column rename without /', () => {
    assert.throws(() => parseRename('column:table.old_col:new_col'), /must use \//);
  });

  it('throws on unknown rename kind', () => {
    assert.throws(() => parseRename('index:old:new'), /Unknown rename kind/);
  });
});
