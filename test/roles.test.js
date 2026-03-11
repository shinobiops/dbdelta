import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { introspectRoles } from '../src/introspector/roles.js';
import { setupTestDatabases, teardownTestDatabases, getTestUrls, connectTo } from './helpers/db.js';

const { Client } = pg;
const ADMIN_URL = process.env.DBDELTA_TEST_ADMIN_URL || 'postgres://postgres:postgres@localhost:5433/postgres';

describe('introspectRoles', () => {
  let client;
  let adminClient;

  before(async () => {
    await setupTestDatabases();
    const { fromUrl } = getTestUrls();
    client = await connectTo(fromUrl);

    // Roles are cluster-wide, so we create/cleanup via admin connection
    adminClient = new Client({ connectionString: ADMIN_URL });
    await adminClient.connect();

    // Clean up any leftover test roles
    try { await adminClient.query('drop role if exists dbdelta_test_role'); } catch { /* ignore */ }
    try { await adminClient.query('drop role if exists dbdelta_test_login'); } catch { /* ignore */ }

    await adminClient.query('create role dbdelta_test_role nologin');
    await adminClient.query('create role dbdelta_test_login login');
  });

  after(async () => {
    if (adminClient) {
      try { await adminClient.query('drop role if exists dbdelta_test_role'); } catch { /* ignore */ }
      try { await adminClient.query('drop role if exists dbdelta_test_login'); } catch { /* ignore */ }
      await adminClient.end();
    }
    if (client) await client.end();
    await teardownTestDatabases();
  });

  it('returns roles excluding pg_ system roles', async () => {
    const results = await introspectRoles(client, ['public']);
    for (const r of results) {
      assert.ok(!r.definition.name.startsWith('pg_'), 'should exclude pg_ system roles');
    }
  });

  it('finds test roles', async () => {
    const results = await introspectRoles(client, ['public']);
    const names = results.map(r => r.definition.name);
    assert.ok(names.includes('dbdelta_test_role'), 'should find dbdelta_test_role');
    assert.ok(names.includes('dbdelta_test_login'), 'should find dbdelta_test_login');
  });

  it('has correct identity type and schema', async () => {
    const results = await introspectRoles(client, ['public']);
    const role = results.find(r => r.definition.name === 'dbdelta_test_role');
    assert.equal(role.identity.type, 'role');
    assert.equal(role.identity.schema, 'pg_global');
  });

  it('captures login attribute', async () => {
    const results = await introspectRoles(client, ['public']);
    const noLogin = results.find(r => r.definition.name === 'dbdelta_test_role');
    const withLogin = results.find(r => r.definition.name === 'dbdelta_test_login');
    assert.equal(noLogin.definition.login, false);
    assert.equal(withLogin.definition.login, true);
  });

  it('ddl create includes role name', async () => {
    const results = await introspectRoles(client, ['public']);
    const role = results.find(r => r.definition.name === 'dbdelta_test_role');
    assert.ok(role.ddl.create.includes('create role'));
    assert.ok(role.ddl.create.includes('dbdelta_test_role'));
  });

  it('ddl drop includes drop role', async () => {
    const results = await introspectRoles(client, ['public']);
    const role = results.find(r => r.definition.name === 'dbdelta_test_role');
    assert.ok(role.ddl.drop.includes('drop role'));
    assert.ok(role.ddl.drop.includes('dbdelta_test_role'));
  });

  it('login role ddl includes login attribute', async () => {
    const results = await introspectRoles(client, ['public']);
    const role = results.find(r => r.definition.name === 'dbdelta_test_login');
    assert.ok(role.ddl.create.includes('login'));
  });
});
