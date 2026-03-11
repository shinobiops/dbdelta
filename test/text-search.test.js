import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { introspectTextSearch } from '../src/introspector/text-search.js';
import { setupTestDatabases, teardownTestDatabases, getTestUrls, connectTo } from './helpers/db.js';

describe('introspectTextSearch', () => {
  let client;

  before(async () => {
    await setupTestDatabases();
    const { fromUrl } = getTestUrls();
    client = await connectTo(fromUrl);

    // Create a text search dictionary using a built-in template
    await client.query(`
      create text search dictionary public.my_dict (
        template = pg_catalog.simple,
        stopwords = 'english'
      )
    `);

    // Create a text search configuration
    await client.query(`
      create text search configuration public.my_config (parser = pg_catalog."default")
    `);
  });

  after(async () => {
    if (client) await client.end();
    await teardownTestDatabases();
  });

  it('returns text search objects', async () => {
    const results = await introspectTextSearch(client, ['public']);
    assert.ok(results.length > 0, 'should find text search objects');
  });

  it('finds ts_config', async () => {
    const results = await introspectTextSearch(client, ['public']);
    const cfg = results.find(r => r.identity.type === 'ts_config' && r.definition.name === 'my_config');
    assert.ok(cfg, 'should find my_config');
  });

  it('finds ts_dictionary', async () => {
    const results = await introspectTextSearch(client, ['public']);
    const dict = results.find(r => r.identity.type === 'ts_dictionary' && r.definition.name === 'my_dict');
    assert.ok(dict, 'should find my_dict');
  });

  it('ts_config has correct identity', async () => {
    const results = await introspectTextSearch(client, ['public']);
    const cfg = results.find(r => r.identity.type === 'ts_config');
    assert.equal(cfg.identity.type, 'ts_config');
    assert.equal(cfg.identity.schema, 'public');
  });

  it('ts_dictionary captures properties', async () => {
    const results = await introspectTextSearch(client, ['public']);
    const dict = results.find(r => r.definition.name === 'my_dict');
    assert.equal(dict.definition.template_name, 'simple');
    assert.ok(dict.definition.options.includes('stopwords'));
  });

  it('ts_config ddl create includes configuration', async () => {
    const results = await introspectTextSearch(client, ['public']);
    const cfg = results.find(r => r.definition.name === 'my_config');
    assert.ok(cfg.ddl.create.includes('create text search configuration'));
    assert.ok(cfg.ddl.create.includes('my_config'));
  });

  it('ts_dictionary ddl create includes dictionary', async () => {
    const results = await introspectTextSearch(client, ['public']);
    const dict = results.find(r => r.definition.name === 'my_dict');
    assert.ok(dict.ddl.create.includes('create text search dictionary'));
    assert.ok(dict.ddl.create.includes('simple'));
  });

  it('returns all types as flat array', async () => {
    const results = await introspectTextSearch(client, ['public']);
    const types = new Set(results.map(r => r.identity.type));
    assert.ok(types.has('ts_config'), 'should contain ts_config');
    assert.ok(types.has('ts_dictionary'), 'should contain ts_dictionary');
  });
});
