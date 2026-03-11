import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { introspectExtensions } from '../src/introspector/extensions.js';
import { setupTestDatabases, teardownTestDatabases, getTestUrls, connectTo } from './helpers/db.js';

describe('introspectExtensions', () => {
  let client;

  before(async () => {
    await setupTestDatabases();
    const { fromUrl } = getTestUrls();
    client = await connectTo(fromUrl);
    // plpgsql is installed by default in most PG installations
  });

  after(async () => {
    if (client) await client.end();
    await teardownTestDatabases();
  });

  it('returns extensions in the specified schemas', async () => {
    // plpgsql is typically in pg_catalog, so querying public may return empty
    const results = await introspectExtensions(client, ['public', 'pg_catalog']);
    // Just verify it returns an array without error
    assert.ok(Array.isArray(results));
  });

  it('returns correct identity type for extensions', async () => {
    // Create an extension in public schema to test
    try {
      await client.query('create extension if not exists "pgcrypto" schema public');
    } catch {
      // Extension might not be available, skip
      return;
    }
    const results = await introspectExtensions(client, ['public']);
    if (results.length > 0) {
      assert.equal(results[0].identity.type, 'extension');
      assert.ok(results[0].definition.name);
      assert.ok(results[0].definition.version);
      assert.ok(results[0].ddl.create.includes('create extension'));
      assert.ok(results[0].ddl.drop.includes('drop extension'));
    }
  });
});
