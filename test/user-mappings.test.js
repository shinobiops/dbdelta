import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { introspectUserMappings } from '../src/introspector/user-mappings.js';
import { setupTestDatabases, teardownTestDatabases, getTestUrls, connectTo } from './helpers/db.js';

describe('introspectUserMappings', () => {
  let client;

  before(async () => {
    await setupTestDatabases();
    const { fromUrl } = getTestUrls();
    client = await connectTo(fromUrl);

    await client.query('create extension if not exists postgres_fdw');
    await client.query(`
      create server um_test_server
      foreign data wrapper postgres_fdw
      options (host 'localhost', port '5433', dbname 'postgres')
    `);
    await client.query(`
      create user mapping for current_user
      server um_test_server
      options (user 'postgres', password 'postgres')
    `);
  });

  after(async () => {
    if (client) await client.end();
    await teardownTestDatabases();
  });

  it('returns user mappings', async () => {
    const results = await introspectUserMappings(client, ['public']);
    assert.ok(Array.isArray(results));
    assert.ok(results.length > 0, 'should find at least one user mapping');
  });

  it('has correct identity type', async () => {
    const results = await introspectUserMappings(client, ['public']);
    for (const r of results) {
      assert.equal(r.identity.type, 'user_mapping');
    }
  });

  it('identity schema is pg_global', async () => {
    const results = await introspectUserMappings(client, ['public']);
    for (const r of results) {
      assert.equal(r.identity.schema, 'pg_global');
    }
  });

  it('identity name includes server and user', async () => {
    const results = await introspectUserMappings(client, ['public']);
    const mapping = results.find(r => r.definition.server_name === 'um_test_server');
    assert.ok(mapping, 'should find mapping for um_test_server');
    assert.ok(mapping.identity.name.includes('um_test_server'));
    assert.ok(mapping.identity.name.includes('/'));
  });

  it('ddl create/drop are present', async () => {
    const results = await introspectUserMappings(client, ['public']);
    for (const r of results) {
      assert.ok(r.ddl.create.includes('create user mapping'));
      assert.ok(r.ddl.drop.includes('drop user mapping'));
    }
  });
});
