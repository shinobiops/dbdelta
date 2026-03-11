import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { introspectViews } from '../src/introspector/views.js';
import { setupTestDatabases, teardownTestDatabases, getTestUrls, connectTo } from './helpers/db.js';

describe('introspectViews', () => {
  let client;

  before(async () => {
    await setupTestDatabases();
    const { fromUrl } = getTestUrls();
    client = await connectTo(fromUrl);

    await client.query(`create table public.users (id serial primary key, name text, active boolean default true)`);
    await client.query(`create view public.active_users as select id, name from public.users where active = true`);
  });

  after(async () => {
    if (client) await client.end();
    await teardownTestDatabases();
  });

  it('returns views', async () => {
    const results = await introspectViews(client, ['public']);
    const names = results.map(r => r.identity.name);
    assert.ok(names.includes('active_users'), 'should find active_users');
  });

  it('has correct identity type', async () => {
    const results = await introspectViews(client, ['public']);
    const v = results.find(r => r.identity.name === 'active_users');
    assert.equal(v.identity.type, 'view');
  });

  it('captures view definition', async () => {
    const results = await introspectViews(client, ['public']);
    const v = results.find(r => r.identity.name === 'active_users');
    assert.ok(v.definition.view_definition.includes('users'), 'definition should reference users table');
  });

  it('ddl createOrReplace includes create or replace', async () => {
    const results = await introspectViews(client, ['public']);
    const v = results.find(r => r.identity.name === 'active_users');
    assert.ok(v.ddl.createOrReplace.includes('create or replace view'));
  });

  it('does not include materialized views', async () => {
    await client.query(`create materialized view public.mat_users as select id, name from public.users`);
    const results = await introspectViews(client, ['public']);
    const names = results.map(r => r.identity.name);
    assert.ok(!names.includes('mat_users'), 'should not include materialized views');
  });
});
