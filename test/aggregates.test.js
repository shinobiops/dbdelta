import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { introspectAggregates } from '../src/introspector/aggregates.js';
import { setupTestDatabases, teardownTestDatabases, getTestUrls, connectTo } from './helpers/db.js';

describe('introspectAggregates', () => {
  let client;

  before(async () => {
    await setupTestDatabases();
    const { fromUrl } = getTestUrls();
    client = await connectTo(fromUrl);

    // Create a state function for the aggregate
    await client.query(`
      create function public.concat_sfunc(state text, val text)
      returns text
      language sql
      immutable
      as $$ select coalesce(state, '') || coalesce(val, '') $$
    `);

    await client.query(`
      create aggregate public.my_concat(text) (
        sfunc = public.concat_sfunc,
        stype = text,
        initcond = ''
      )
    `);
  });

  after(async () => {
    if (client) await client.end();
    await teardownTestDatabases();
  });

  it('returns aggregates', async () => {
    const results = await introspectAggregates(client, ['public']);
    assert.ok(results.length > 0, 'should find at least one aggregate');
    const agg = results.find(r => r.definition.name === 'my_concat');
    assert.ok(agg, 'should find my_concat');
  });

  it('has correct identity type', async () => {
    const results = await introspectAggregates(client, ['public']);
    const agg = results.find(r => r.definition.name === 'my_concat');
    assert.equal(agg.identity.type, 'aggregate');
  });

  it('identity name includes argument types', async () => {
    const results = await introspectAggregates(client, ['public']);
    const agg = results.find(r => r.definition.name === 'my_concat');
    assert.ok(agg.identity.name.includes('text'), 'should include arg types');
  });

  it('captures aggregate properties', async () => {
    const results = await introspectAggregates(client, ['public']);
    const agg = results.find(r => r.definition.name === 'my_concat');
    assert.equal(agg.definition.state_type, 'text');
    assert.equal(agg.definition.init_val, '');
    assert.ok(agg.definition.state_func.includes('concat_sfunc'));
  });

  it('ddl create includes aggregate definition', async () => {
    const results = await introspectAggregates(client, ['public']);
    const agg = results.find(r => r.definition.name === 'my_concat');
    assert.ok(agg.ddl.create.includes('my_concat'));
    assert.ok(agg.ddl.create.includes('sfunc'));
    assert.ok(agg.ddl.create.includes('stype'));
  });

  it('ddl drop includes arg types', async () => {
    const results = await introspectAggregates(client, ['public']);
    const agg = results.find(r => r.definition.name === 'my_concat');
    assert.ok(agg.ddl.drop.includes('text'));
  });
});
