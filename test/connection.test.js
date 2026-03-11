import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { connect } from '../src/connection.js';
import { setupTestDatabases, teardownTestDatabases, getTestUrls } from './helpers/db.js';

describe('connection', () => {
  before(async () => { await setupTestDatabases(); });
  after(async () => { await teardownTestDatabases(); });

  it('connects to a postgres database and runs a query', async () => {
    const { fromUrl } = getTestUrls();
    const client = await connect(fromUrl);
    try {
      const result = await client.query('select 1 as num');
      assert.equal(result.rows[0].num, 1);
    } finally {
      await client.end();
    }
  });
});
