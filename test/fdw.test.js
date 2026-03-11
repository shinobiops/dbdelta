import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { introspectFdw } from '../src/introspector/fdw.js';
import { setupTestDatabases, teardownTestDatabases, getTestUrls, connectTo } from './helpers/db.js';

describe('introspectFdw', () => {
  let client;

  before(async () => {
    await setupTestDatabases();
    const { fromUrl } = getTestUrls();
    client = await connectTo(fromUrl);

    await client.query('create extension if not exists postgres_fdw');

    await client.query(`
      create server test_fdw_server
      foreign data wrapper postgres_fdw
      options (host 'localhost', port '5433', dbname 'postgres')
    `);

    await client.query(`
      create foreign table public.remote_items (
        id integer,
        name text
      ) server test_fdw_server
      options (schema_name 'public', table_name 'items')
    `);
  });

  after(async () => {
    if (client) await client.end();
    await teardownTestDatabases();
  });

  it('returns fdw entries', async () => {
    const results = await introspectFdw(client, ['public']);
    const fdws = results.filter(r => r.identity.type === 'fdw');
    const names = fdws.map(r => r.definition.name);
    assert.ok(names.includes('postgres_fdw'), 'should find postgres_fdw');
  });

  it('returns foreign servers', async () => {
    const results = await introspectFdw(client, ['public']);
    const servers = results.filter(r => r.identity.type === 'foreign_server');
    const names = servers.map(r => r.definition.name);
    assert.ok(names.includes('test_fdw_server'), 'should find test_fdw_server');
  });

  it('foreign server references fdw', async () => {
    const results = await introspectFdw(client, ['public']);
    const server = results.find(r => r.identity.type === 'foreign_server' && r.definition.name === 'test_fdw_server');
    assert.equal(server.definition.fdw_name, 'postgres_fdw');
  });

  it('returns foreign tables', async () => {
    const results = await introspectFdw(client, ['public']);
    const tables = results.filter(r => r.identity.type === 'foreign_table');
    const names = tables.map(r => r.definition.name);
    assert.ok(names.includes('remote_items'), 'should find remote_items');
  });

  it('foreign table has correct schema', async () => {
    const results = await introspectFdw(client, ['public']);
    const ft = results.find(r => r.identity.type === 'foreign_table' && r.definition.name === 'remote_items');
    assert.equal(ft.identity.schema, 'public');
    assert.equal(ft.definition.server_name, 'test_fdw_server');
  });

  it('fdw identity uses pg_global schema', async () => {
    const results = await introspectFdw(client, ['public']);
    const fdw = results.find(r => r.identity.type === 'fdw');
    assert.equal(fdw.identity.schema, 'pg_global');
  });

  it('ddl create/drop are present', async () => {
    const results = await introspectFdw(client, ['public']);
    for (const r of results) {
      assert.ok(r.ddl.create, `missing create DDL for ${r.identity.name}`);
      assert.ok(r.ddl.drop, `missing drop DDL for ${r.identity.name}`);
    }
  });

  it('foreign table ddl includes server name', async () => {
    const results = await introspectFdw(client, ['public']);
    const ft = results.find(r => r.identity.type === 'foreign_table' && r.definition.name === 'remote_items');
    assert.ok(ft.ddl.create.includes('test_fdw_server'));
  });
});
