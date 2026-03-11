import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { introspectIndexes } from '../src/introspector/indexes.js';
import { setupTestDatabases, teardownTestDatabases, getTestUrls, connectTo } from './helpers/db.js';

describe('introspectIndexes', () => {
  let client;

  before(async () => {
    await setupTestDatabases();
    const { fromUrl } = getTestUrls();
    client = await connectTo(fromUrl);

    await client.query(`
      create table public.idx_test (
        id integer primary key,
        name text,
        email text,
        active boolean
      )
    `);
    await client.query(`create index idx_name on public.idx_test (name)`);
    await client.query(`create unique index idx_email on public.idx_test (email)`);
    await client.query(`create index idx_active_name on public.idx_test (name) where active = true`);
  });

  after(async () => {
    if (client) await client.end();
    await teardownTestDatabases();
  });

  it('returns indexes but excludes primary key', async () => {
    const results = await introspectIndexes(client, ['public']);
    const names = results.map(r => r.identity.name);
    assert.ok(names.includes('idx_name'), 'should include idx_name');
    assert.ok(names.includes('idx_email'), 'should include idx_email');
    // Primary key index should be excluded
    const pkIndex = results.find(r => r.definition.table === 'idx_test' && r.identity.name.includes('pkey'));
    assert.ok(!pkIndex, 'should not include primary key index');
  });

  it('detects unique indexes', async () => {
    const results = await introspectIndexes(client, ['public']);
    const emailIdx = results.find(r => r.identity.name === 'idx_email');
    assert.equal(emailIdx.definition.is_unique, true);
  });

  it('detects partial indexes', async () => {
    const results = await introspectIndexes(client, ['public']);
    const partialIdx = results.find(r => r.identity.name === 'idx_active_name');
    assert.ok(partialIdx, 'should find partial index');
    assert.ok(partialIdx.definition.predicate, 'should have predicate');
  });

  it('captures access method', async () => {
    const results = await introspectIndexes(client, ['public']);
    const idx = results.find(r => r.identity.name === 'idx_name');
    assert.equal(idx.definition.access_method, 'btree');
  });

  it('has correct identity type', async () => {
    const results = await introspectIndexes(client, ['public']);
    const idx = results.find(r => r.identity.name === 'idx_name');
    assert.equal(idx.identity.type, 'index');
  });

  it('ddl create is the full indexdef', async () => {
    const results = await introspectIndexes(client, ['public']);
    const idx = results.find(r => r.identity.name === 'idx_name');
    assert.ok(idx.ddl.create.includes('CREATE INDEX'));
    assert.ok(idx.ddl.create.endsWith(';'));
  });
});
