import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { introspectTables } from '../src/introspector/tables.js';
import { setupTestDatabases, teardownTestDatabases, getTestUrls, connectTo } from './helpers/db.js';

describe('introspectTables', () => {
  let client;

  before(async () => {
    await setupTestDatabases();
    const { fromUrl } = getTestUrls();
    client = await connectTo(fromUrl);

    await client.query(`create table public.users (id integer, name text)`);
    await client.query(`create unlogged table public.sessions (id integer, data text)`);
    await client.query(`create table public.logs (id integer, created_at timestamp) partition by range (created_at)`);
  });

  after(async () => {
    if (client) await client.end();
    await teardownTestDatabases();
  });

  it('returns regular tables', async () => {
    const results = await introspectTables(client, ['public']);
    const users = results.find(r => r.identity.name === 'users');
    assert.ok(users, 'should find users table');
    assert.equal(users.identity.type, 'table');
    assert.equal(users.definition.persistence, 'permanent');
  });

  it('detects unlogged tables', async () => {
    const results = await introspectTables(client, ['public']);
    const sessions = results.find(r => r.identity.name === 'sessions');
    assert.ok(sessions, 'should find sessions table');
    assert.equal(sessions.definition.persistence, 'unlogged');
    assert.ok(sessions.ddl.create.includes('unlogged'));
  });

  it('detects partitioned tables', async () => {
    const results = await introspectTables(client, ['public']);
    const logs = results.find(r => r.identity.name === 'logs');
    assert.ok(logs, 'should find logs table');
    assert.ok(logs.definition.partition_key, 'should have partition key');
    assert.ok(logs.ddl.create.includes('partition by'));
  });

  it('includes ddl create statement', async () => {
    const results = await introspectTables(client, ['public']);
    const users = results.find(r => r.identity.name === 'users');
    assert.ok(users.ddl.create.includes('create table'));
    assert.ok(users.ddl.create.includes('public.users'));
  });

  it('captures owner', async () => {
    const results = await introspectTables(client, ['public']);
    const users = results.find(r => r.identity.name === 'users');
    assert.ok(users.definition.owner, 'should have an owner');
  });
});
