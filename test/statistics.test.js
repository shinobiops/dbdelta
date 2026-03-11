import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { introspectStatistics } from '../src/introspector/statistics.js';
import { setupTestDatabases, teardownTestDatabases, getTestUrls, connectTo } from './helpers/db.js';

describe('introspectStatistics', () => {
  let client;

  before(async () => {
    await setupTestDatabases();
    const { fromUrl } = getTestUrls();
    client = await connectTo(fromUrl);

    await client.query(`
      create table public.stat_test (
        a integer,
        b integer,
        c text
      )
    `);

    await client.query(`
      create statistics public.my_stats (dependencies, ndistinct) on a, b from public.stat_test
    `);
  });

  after(async () => {
    if (client) await client.end();
    await teardownTestDatabases();
  });

  it('returns statistics', async () => {
    const results = await introspectStatistics(client, ['public']);
    assert.ok(results.length > 0, 'should find at least one statistics object');
    const stat = results.find(r => r.definition.name === 'my_stats');
    assert.ok(stat, 'should find my_stats');
  });

  it('has correct identity type', async () => {
    const results = await introspectStatistics(client, ['public']);
    const stat = results.find(r => r.definition.name === 'my_stats');
    assert.equal(stat.identity.type, 'statistics');
  });

  it('captures statistics properties', async () => {
    const results = await introspectStatistics(client, ['public']);
    const stat = results.find(r => r.definition.name === 'my_stats');
    assert.ok(stat.definition.kinds.includes('dependencies'));
    assert.ok(stat.definition.kinds.includes('ndistinct'));
    assert.deepEqual(stat.definition.columns, ['a', 'b']);
    assert.equal(stat.definition.table_name, 'stat_test');
  });

  it('ddl create includes statistics definition', async () => {
    const results = await introspectStatistics(client, ['public']);
    const stat = results.find(r => r.definition.name === 'my_stats');
    assert.ok(stat.ddl.create.includes('my_stats'));
    assert.ok(stat.ddl.create.includes('dependencies'));
    assert.ok(stat.ddl.create.includes('ndistinct'));
    assert.ok(stat.ddl.create.includes('stat_test'));
  });

  it('ddl drop includes statistics name', async () => {
    const results = await introspectStatistics(client, ['public']);
    const stat = results.find(r => r.definition.name === 'my_stats');
    assert.ok(stat.ddl.drop.includes('my_stats'));
  });
});
