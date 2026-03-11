import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { introspectRules } from '../src/introspector/rules.js';
import { setupTestDatabases, teardownTestDatabases, getTestUrls, connectTo } from './helpers/db.js';

describe('introspectRules', () => {
  let client;

  before(async () => {
    await setupTestDatabases();
    const { fromUrl } = getTestUrls();
    client = await connectTo(fromUrl);

    await client.query(`create table public.log_entries (id serial, message text, created_at timestamptz default now())`);
    await client.query(`create table public.log_archive (id serial, message text, created_at timestamptz)`);

    await client.query(`
      create rule log_to_archive as
      on insert to public.log_entries
      do also
      insert into public.log_archive (message, created_at) values (new.message, new.created_at)
    `);
  });

  after(async () => {
    if (client) await client.end();
    await teardownTestDatabases();
  });

  it('returns rules', async () => {
    const results = await introspectRules(client, ['public']);
    const names = results.map(r => r.definition.name);
    assert.ok(names.includes('log_to_archive'), 'should find log_to_archive');
  });

  it('has correct identity type', async () => {
    const results = await introspectRules(client, ['public']);
    const rule = results.find(r => r.definition.name === 'log_to_archive');
    assert.equal(rule.identity.type, 'rule');
  });

  it('identity name includes table name', async () => {
    const results = await introspectRules(client, ['public']);
    const rule = results.find(r => r.definition.name === 'log_to_archive');
    assert.equal(rule.identity.name, 'log_to_archive.on.log_entries');
  });

  it('captures rule properties', async () => {
    const results = await introspectRules(client, ['public']);
    const rule = results.find(r => r.definition.name === 'log_to_archive');
    assert.equal(rule.definition.table_name, 'log_entries');
  });

  it('ddl create includes rule definition', async () => {
    const results = await introspectRules(client, ['public']);
    const rule = results.find(r => r.definition.name === 'log_to_archive');
    assert.ok(rule.ddl.create.includes('log_to_archive'));
  });

  it('ddl drop includes drop rule', async () => {
    const results = await introspectRules(client, ['public']);
    const rule = results.find(r => r.definition.name === 'log_to_archive');
    assert.ok(rule.ddl.drop.includes('drop rule'));
  });

  it('excludes _RETURN rules', async () => {
    // _RETURN rules are auto-created for views
    await client.query(`create view public.test_rule_view as select 1 as n`);
    const results = await introspectRules(client, ['public']);
    const names = results.map(r => r.definition.name);
    assert.ok(!names.includes('_RETURN'), 'should not include _RETURN rules');
  });
});
