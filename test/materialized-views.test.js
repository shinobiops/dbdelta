import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { introspectMaterializedViews } from '../src/introspector/materialized-views.js';
import { setupTestDatabases, teardownTestDatabases, getTestUrls, connectTo } from './helpers/db.js';

describe('introspectMaterializedViews', () => {
  let client;

  before(async () => {
    await setupTestDatabases();
    const { fromUrl } = getTestUrls();
    client = await connectTo(fromUrl);

    await client.query(`create table public.products (id serial primary key, name text, price numeric)`);
    await client.query(`create materialized view public.product_summary as select count(*) as total, sum(price) as total_price from public.products`);
  });

  after(async () => {
    if (client) await client.end();
    await teardownTestDatabases();
  });

  it('returns materialized views', async () => {
    const results = await introspectMaterializedViews(client, ['public']);
    const names = results.map(r => r.identity.name);
    assert.ok(names.includes('product_summary'), 'should find product_summary');
  });

  it('has correct identity type', async () => {
    const results = await introspectMaterializedViews(client, ['public']);
    const mv = results.find(r => r.identity.name === 'product_summary');
    assert.equal(mv.identity.type, 'materialized_view');
  });

  it('captures view definition', async () => {
    const results = await introspectMaterializedViews(client, ['public']);
    const mv = results.find(r => r.identity.name === 'product_summary');
    assert.ok(mv.definition.view_definition.includes('products'), 'definition should reference products table');
  });

  it('ddl create includes with no data', async () => {
    const results = await introspectMaterializedViews(client, ['public']);
    const mv = results.find(r => r.identity.name === 'product_summary');
    assert.ok(mv.ddl.create.includes('create materialized view'));
    assert.ok(mv.ddl.create.includes('with no data'));
  });

  it('does not include regular views', async () => {
    await client.query(`create view public.simple_view as select 1 as num`);
    const results = await introspectMaterializedViews(client, ['public']);
    const names = results.map(r => r.identity.name);
    assert.ok(!names.includes('simple_view'), 'should not include regular views');
  });
});
