import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { introspectCollations } from '../src/introspector/collations.js';
import { setupTestDatabases, teardownTestDatabases, getTestUrls, connectTo } from './helpers/db.js';

describe('introspectCollations', () => {
  let client;

  before(async () => {
    await setupTestDatabases();
    const { fromUrl } = getTestUrls();
    client = await connectTo(fromUrl);

    await client.query(`
      create collation public.my_collation (provider = libc, locale = 'C')
    `);
  });

  after(async () => {
    if (client) await client.end();
    await teardownTestDatabases();
  });

  it('returns collations', async () => {
    const results = await introspectCollations(client, ['public']);
    assert.ok(results.length > 0, 'should find at least one collation');
    const coll = results.find(r => r.definition.name === 'my_collation');
    assert.ok(coll, 'should find my_collation');
  });

  it('has correct identity type', async () => {
    const results = await introspectCollations(client, ['public']);
    const coll = results.find(r => r.definition.name === 'my_collation');
    assert.equal(coll.identity.type, 'collation');
  });

  it('captures collation properties', async () => {
    const results = await introspectCollations(client, ['public']);
    const coll = results.find(r => r.definition.name === 'my_collation');
    assert.equal(coll.definition.provider, 'libc');
    assert.equal(coll.definition.lc_collate, 'C');
  });

  it('ddl create includes collation definition', async () => {
    const results = await introspectCollations(client, ['public']);
    const coll = results.find(r => r.definition.name === 'my_collation');
    assert.ok(coll.ddl.create.includes('my_collation'));
    assert.ok(coll.ddl.create.includes('libc'));
  });

  it('ddl drop includes collation name', async () => {
    const results = await introspectCollations(client, ['public']);
    const coll = results.find(r => r.definition.name === 'my_collation');
    assert.ok(coll.ddl.drop.includes('my_collation'));
  });
});
