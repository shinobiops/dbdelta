import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { parseRenames, matchObjects, determineChangeStrategy, diff, deepEqual } from '../src/differ.js';

describe('parseRenames', () => {
  it('parses table renames', () => {
    const renames = parseRenames(['table:users:accounts']);
    assert.deepEqual(renames.tables.get('public.users'), 'accounts');
  });

  it('parses column renames with schema', () => {
    const renames = parseRenames(['column:myschema.users/email:email_address']);
    assert.deepEqual(renames.columns.get('myschema.users.email'), 'email_address');
  });

  it('defaults column rename schema to public', () => {
    const renames = parseRenames(['column:users/email:email_address']);
    assert.deepEqual(renames.columns.get('public.users.email'), 'email_address');
  });

  it('handles multiple renames', () => {
    const renames = parseRenames([
      'table:old_t:new_t',
      'column:t1/old_c:new_c',
    ]);
    assert.equal(renames.tables.size, 1);
    assert.equal(renames.columns.size, 1);
  });

  it('throws on invalid format', () => {
    assert.throws(() => parseRenames(['bad']), /invalid rename/i);
  });

  it('returns empty maps for null/empty input', () => {
    const r1 = parseRenames(null);
    assert.equal(r1.tables.size, 0);
    assert.equal(r1.columns.size, 0);

    const r2 = parseRenames([]);
    assert.equal(r2.tables.size, 0);
    assert.equal(r2.columns.size, 0);
  });
});

describe('matchObjects', () => {
  it('matches objects by schema+type+name', () => {
    const from = [
      { identity: { schema: 'public', type: 'table', name: 'users' }, definition: { columns: [] } },
    ];
    const to = [
      { identity: { schema: 'public', type: 'table', name: 'users' }, definition: { columns: [{ name: 'id' }] } },
    ];
    const { matched, createOnly, dropOnly } = matchObjects(from, to, { tables: new Map(), columns: new Map() });
    assert.equal(matched.length, 1);
    assert.equal(createOnly.length, 0);
    assert.equal(dropOnly.length, 0);
  });

  it('applies table renames when matching', () => {
    const from = [
      { identity: { schema: 'public', type: 'table', name: 'users' }, definition: {} },
    ];
    const to = [
      { identity: { schema: 'public', type: 'table', name: 'accounts' }, definition: {} },
    ];
    const renames = { tables: new Map([['public.users', 'accounts']]), columns: new Map() };
    const { matched, createOnly, dropOnly } = matchObjects(from, to, renames);
    assert.equal(matched.length, 1);
    assert.equal(matched[0].renamed, true);
    assert.equal(createOnly.length, 0);
    assert.equal(dropOnly.length, 0);
  });

  it('classifies unmatched objects', () => {
    const from = [
      { identity: { schema: 'public', type: 'table', name: 'old_table' }, definition: {} },
    ];
    const to = [
      { identity: { schema: 'public', type: 'table', name: 'new_table' }, definition: {} },
    ];
    const renames = { tables: new Map(), columns: new Map() };
    const { matched, createOnly, dropOnly } = matchObjects(from, to, renames);
    assert.equal(matched.length, 0);
    assert.equal(createOnly.length, 1);
    assert.equal(dropOnly.length, 1);
  });
});

describe('deepEqual', () => {
  it('returns true for identical objects', () => {
    assert.equal(deepEqual({ a: 1, b: [2, 3] }, { a: 1, b: [2, 3] }), true);
  });

  it('returns false for different objects', () => {
    assert.equal(deepEqual({ a: 1 }, { a: 2 }), false);
  });

  it('handles nested objects', () => {
    assert.equal(deepEqual({ a: { b: { c: 1 } } }, { a: { b: { c: 1 } } }), true);
    assert.equal(deepEqual({ a: { b: { c: 1 } } }, { a: { b: { c: 2 } } }), false);
  });

  it('handles nulls', () => {
    assert.equal(deepEqual(null, null), true);
    assert.equal(deepEqual(null, {}), false);
  });

  it('handles primitives', () => {
    assert.equal(deepEqual(1, 1), true);
    assert.equal(deepEqual('abc', 'abc'), true);
    assert.equal(deepEqual(1, 2), false);
    assert.equal(deepEqual(true, false), false);
  });

  it('handles arrays of different lengths', () => {
    assert.equal(deepEqual([1, 2], [1, 2, 3]), false);
  });

  it('handles different key counts', () => {
    assert.equal(deepEqual({ a: 1 }, { a: 1, b: 2 }), false);
  });
});

describe('determineChangeStrategy', () => {
  it('returns ALTER for tables with column changes', () => {
    assert.equal(determineChangeStrategy('table', {}, {}), 'ALTER');
  });

  it('returns CREATE_OR_REPLACE for functions', () => {
    assert.equal(
      determineChangeStrategy('function', { body: 'old' }, { body: 'new' }),
      'CREATE_OR_REPLACE'
    );
  });

  it('returns CREATE_OR_REPLACE for views', () => {
    assert.equal(
      determineChangeStrategy('view', { query: 'old' }, { query: 'new' }),
      'CREATE_OR_REPLACE'
    );
  });

  it('returns DROP_AND_CREATE for indexes', () => {
    assert.equal(determineChangeStrategy('index', {}, {}), 'DROP_AND_CREATE');
  });

  it('returns DROP_AND_CREATE for triggers', () => {
    assert.equal(determineChangeStrategy('trigger', {}, {}), 'DROP_AND_CREATE');
  });

  it('returns DROP_AND_CREATE for materialized views', () => {
    assert.equal(determineChangeStrategy('materialized_view', {}, {}), 'DROP_AND_CREATE');
  });

  it('returns ALTER for enum adding values', () => {
    const from = { labels: ['a', 'b'] };
    const to = { labels: ['a', 'b', 'c'] };
    assert.equal(determineChangeStrategy('enum', from, to), 'ALTER');
  });

  it('returns DROP_AND_CREATE for enum removing values', () => {
    const from = { labels: ['a', 'b', 'c'] };
    const to = { labels: ['a', 'b'] };
    assert.equal(determineChangeStrategy('enum', from, to), 'DROP_AND_CREATE');
  });

  it('returns DROP_AND_CREATE for constraints', () => {
    assert.equal(determineChangeStrategy('constraint', {}, {}), 'DROP_AND_CREATE');
  });

  it('returns ALTER for columns', () => {
    assert.equal(determineChangeStrategy('column', {}, {}), 'ALTER');
  });
});

describe('diff', () => {
  it('produces CREATE for new objects', () => {
    const from = [];
    const to = [
      { identity: { schema: 'public', type: 'table', name: 't1' }, definition: { columns: [] }, ddl: {} },
    ];
    const ops = diff(from, to, {});
    assert.equal(ops.length, 1);
    assert.equal(ops[0].op, 'CREATE');
  });

  it('produces DROP for removed objects', () => {
    const from = [
      { identity: { schema: 'public', type: 'table', name: 't1' }, definition: { columns: [] }, ddl: {} },
    ];
    const to = [];
    const ops = diff(from, to, {});
    assert.equal(ops.length, 1);
    assert.equal(ops[0].op, 'DROP');
  });

  it('produces no ops for identical objects', () => {
    const obj = { identity: { schema: 'public', type: 'table', name: 't1' }, definition: { columns: [{ name: 'id', type: 'integer' }] }, ddl: {} };
    const ops = diff([obj], [{ ...obj }], {});
    assert.equal(ops.length, 0);
  });

  it('respects excludeTypes', () => {
    const from = [];
    const to = [
      { identity: { schema: 'public', type: 'table', name: 't1' }, definition: {}, ddl: {} },
      { identity: { schema: 'public', type: 'index', name: 'i1' }, definition: {}, ddl: {} },
    ];
    const ops = diff(from, to, { excludeTypes: ['index'] });
    assert.equal(ops.length, 1);
    assert.equal(ops[0].identity.type, 'table');
  });

  it('respects excludeSchemas', () => {
    const from = [];
    const to = [
      { identity: { schema: 'public', type: 'table', name: 't1' }, definition: {}, ddl: {} },
      { identity: { schema: 'audit', type: 'table', name: 't2' }, definition: {}, ddl: {} },
    ];
    const ops = diff(from, to, { excludeSchemas: ['audit'] });
    assert.equal(ops.length, 1);
    assert.equal(ops[0].identity.schema, 'public');
  });

  it('produces RENAME ops when renames are specified', () => {
    const from = [
      { identity: { schema: 'public', type: 'table', name: 'users' }, definition: { columns: [] }, ddl: {} },
    ];
    const to = [
      { identity: { schema: 'public', type: 'table', name: 'accounts' }, definition: { columns: [] }, ddl: {} },
    ];
    const ops = diff(from, to, { renames: ['table:users:accounts'] });
    const renameOp = ops.find(o => o.op === 'RENAME');
    assert.ok(renameOp);
    assert.equal(renameOp.newName, 'accounts');
  });

  it('produces ALTER for changed definitions on alterable types', () => {
    const from = [
      { identity: { schema: 'public', type: 'column', name: 't1.col1' }, definition: { data_type: 'integer' }, ddl: {} },
    ];
    const to = [
      { identity: { schema: 'public', type: 'column', name: 't1.col1' }, definition: { data_type: 'bigint' }, ddl: {} },
    ];
    const ops = diff(from, to, {});
    assert.equal(ops.length, 1);
    assert.equal(ops[0].op, 'ALTER');
  });

  it('produces DROP_AND_CREATE for changed index', () => {
    const from = [
      { identity: { schema: 'public', type: 'index', name: 'idx1' }, definition: { indexdef: 'old' }, ddl: {} },
    ];
    const to = [
      { identity: { schema: 'public', type: 'index', name: 'idx1' }, definition: { indexdef: 'new' }, ddl: {} },
    ];
    const ops = diff(from, to, {});
    assert.equal(ops.length, 1);
    assert.equal(ops[0].op, 'DROP_AND_CREATE');
  });
});
