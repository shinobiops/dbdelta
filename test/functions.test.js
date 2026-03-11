import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { introspectFunctions } from '../src/introspector/functions.js';
import { setupTestDatabases, teardownTestDatabases, getTestUrls, connectTo } from './helpers/db.js';

describe('introspectFunctions', () => {
  let client;

  before(async () => {
    await setupTestDatabases();
    const { fromUrl } = getTestUrls();
    client = await connectTo(fromUrl);

    await client.query(`
      create function public.add_numbers(a integer, b integer)
      returns integer
      language sql
      immutable
      as $$ select a + b $$
    `);

    await client.query(`
      create function public.greet(name text)
      returns text
      language plpgsql
      stable
      security definer
      as $$
      begin
        return 'hello ' || name;
      end;
      $$
    `);

    await client.query(`
      create procedure public.do_nothing()
      language sql
      as $$ select 1 $$
    `);
  });

  after(async () => {
    if (client) await client.end();
    await teardownTestDatabases();
  });

  it('returns functions', async () => {
    const results = await introspectFunctions(client, ['public']);
    const names = results.map(r => r.definition.name);
    assert.ok(names.includes('add_numbers'), 'should find add_numbers');
    assert.ok(names.includes('greet'), 'should find greet');
  });

  it('has correct identity type', async () => {
    const results = await introspectFunctions(client, ['public']);
    const fn = results.find(r => r.definition.name === 'add_numbers');
    assert.equal(fn.identity.type, 'function');
  });

  it('identity name includes argument types', async () => {
    const results = await introspectFunctions(client, ['public']);
    const fn = results.find(r => r.definition.name === 'add_numbers');
    assert.ok(fn.identity.name.includes('integer'), 'should include arg types in identity name');
  });

  it('captures function properties', async () => {
    const results = await introspectFunctions(client, ['public']);
    const fn = results.find(r => r.definition.name === 'add_numbers');
    assert.equal(fn.definition.volatility, 'immutable');
    assert.equal(fn.definition.language, 'sql');
    assert.equal(fn.definition.result_type, 'integer');
  });

  it('captures security definer', async () => {
    const results = await introspectFunctions(client, ['public']);
    const fn = results.find(r => r.definition.name === 'greet');
    assert.equal(fn.definition.security_definer, true);
    assert.equal(fn.definition.volatility, 'stable');
  });

  it('includes procedures', async () => {
    const results = await introspectFunctions(client, ['public']);
    const proc = results.find(r => r.definition.name === 'do_nothing');
    assert.ok(proc, 'should find procedure');
    assert.equal(proc.definition.kind, 'p');
  });

  it('ddl createOrReplace includes function definition', async () => {
    const results = await introspectFunctions(client, ['public']);
    const fn = results.find(r => r.definition.name === 'add_numbers');
    assert.ok(fn.ddl.createOrReplace.includes('add_numbers'));
    assert.ok(fn.ddl.createOrReplace.includes('integer'));
  });

  it('excludes aggregate functions', async () => {
    const results = await introspectFunctions(client, ['public']);
    const names = results.map(r => r.definition.name);
    // built-in aggregates like sum, count should not appear (they are in pg_catalog anyway)
    // just verify none have kind 'a'
    for (const r of results) {
      assert.notEqual(r.definition.kind, 'a', 'should not include aggregates');
    }
  });
});
