import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { introspectPublications } from '../src/introspector/publications.js';
import { setupTestDatabases, teardownTestDatabases, getTestUrls, connectTo } from './helpers/db.js';

describe('introspectPublications', () => {
  let client;

  before(async () => {
    await setupTestDatabases();
    const { fromUrl } = getTestUrls();
    client = await connectTo(fromUrl);

    await client.query('create table public.pub_test (id serial primary key, name text)');
    await client.query('create publication test_pub for table public.pub_test');
    await client.query('create publication test_pub_all for all tables');
  });

  after(async () => {
    if (client) await client.end();
    await teardownTestDatabases();
  });

  it('returns publications', async () => {
    const results = await introspectPublications(client, ['public']);
    const names = results.map(r => r.definition.name);
    assert.ok(names.includes('test_pub'), 'should find test_pub');
    assert.ok(names.includes('test_pub_all'), 'should find test_pub_all');
  });

  it('has correct identity type', async () => {
    const results = await introspectPublications(client, ['public']);
    for (const r of results) {
      assert.equal(r.identity.type, 'publication');
      assert.equal(r.identity.schema, 'pg_global');
    }
  });

  it('captures all_tables flag', async () => {
    const results = await introspectPublications(client, ['public']);
    const allPub = results.find(r => r.definition.name === 'test_pub_all');
    assert.equal(allPub.definition.all_tables, true);
  });

  it('captures tables list for specific publication', async () => {
    const results = await introspectPublications(client, ['public']);
    const pub = results.find(r => r.definition.name === 'test_pub');
    assert.ok(pub.definition.tables.length > 0, 'should have tables');
    assert.ok(pub.definition.tables.some(t => t.includes('pub_test')));
  });

  it('ddl create includes publication name', async () => {
    const results = await introspectPublications(client, ['public']);
    const pub = results.find(r => r.definition.name === 'test_pub');
    assert.ok(pub.ddl.create.includes('create publication'));
    assert.ok(pub.ddl.create.includes('test_pub'));
  });

  it('ddl drop includes drop publication', async () => {
    const results = await introspectPublications(client, ['public']);
    const pub = results.find(r => r.definition.name === 'test_pub');
    assert.ok(pub.ddl.drop.includes('drop publication'));
  });
});
