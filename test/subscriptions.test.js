import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { introspectSubscriptions } from '../src/introspector/subscriptions.js';
import { setupTestDatabases, teardownTestDatabases, getTestUrls, connectTo } from './helpers/db.js';

describe('introspectSubscriptions', () => {
  let client;

  before(async () => {
    await setupTestDatabases();
    const { fromUrl } = getTestUrls();
    client = await connectTo(fromUrl);
  });

  after(async () => {
    if (client) await client.end();
    await teardownTestDatabases();
  });

  it('returns empty array when no subscriptions exist', async () => {
    // Subscriptions require a valid external connection to create,
    // so we just verify the introspector returns an empty array.
    const results = await introspectSubscriptions(client, ['public']);
    assert.ok(Array.isArray(results));
    assert.equal(results.length, 0);
  });
});
