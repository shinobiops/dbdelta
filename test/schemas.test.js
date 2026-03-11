import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { introspectSchemas } from '../src/introspector/schemas.js';
import { setupTestDatabases, teardownTestDatabases, getTestUrls, connectTo } from './helpers/db.js';

describe('introspectSchemas', () => {
  let client;

  before(async () => {
    await setupTestDatabases();
    const { fromUrl } = getTestUrls();
    client = await connectTo(fromUrl);
    await client.query('create schema if not exists app');
  });

  after(async () => {
    if (client) await client.end();
    await teardownTestDatabases();
  });

  it('returns public and app schemas', async () => {
    const results = await introspectSchemas(client, ['public', 'app']);
    const names = results.map(r => r.identity.name);
    assert.ok(names.includes('public'), 'should include public');
    assert.ok(names.includes('app'), 'should include app');
  });

  it('returns correct identity type', async () => {
    const results = await introspectSchemas(client, ['public']);
    assert.equal(results[0].identity.type, 'schema');
  });

  it('excludes system schemas even if requested', async () => {
    const results = await introspectSchemas(client, ['pg_catalog', 'public']);
    const names = results.map(r => r.identity.name);
    assert.ok(!names.includes('pg_catalog'), 'should not include pg_catalog');
  });

  it('includes ddl with create and drop', async () => {
    const results = await introspectSchemas(client, ['app']);
    assert.ok(results[0].ddl.create.includes('create schema'));
    assert.ok(results[0].ddl.drop.includes('drop schema'));
  });
});
