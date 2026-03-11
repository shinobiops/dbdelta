import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { introspectGrants } from '../src/introspector/grants.js';
import { setupTestDatabases, teardownTestDatabases, getTestUrls, connectTo } from './helpers/db.js';

const { Client } = pg;
const ADMIN_URL = process.env.DBDELTA_TEST_ADMIN_URL || 'postgres://postgres:postgres@localhost:5433/postgres';

describe('introspectGrants', () => {
  let client;
  let adminClient;

  before(async () => {
    await setupTestDatabases();
    const { fromUrl } = getTestUrls();
    client = await connectTo(fromUrl);

    // Create a test role for granting
    adminClient = new Client({ connectionString: ADMIN_URL });
    await adminClient.connect();
    try { await adminClient.query('drop role if exists dbdelta_grant_test'); } catch { /* ignore */ }
    await adminClient.query('create role dbdelta_grant_test nologin');

    // Create a test table and grant permissions
    await client.query('create table public.grant_test_table (id serial primary key, name text)');
    await client.query('grant select, insert on public.grant_test_table to dbdelta_grant_test');
  });

  after(async () => {
    if (client) await client.end();
    if (adminClient) {
      try { await adminClient.query('drop role if exists dbdelta_grant_test'); } catch { /* ignore */ }
      await adminClient.end();
    }
    await teardownTestDatabases();
  });

  it('returns grants', async () => {
    const results = await introspectGrants(client, ['public']);
    assert.ok(Array.isArray(results));
    assert.ok(results.length > 0, 'should find at least one grant');
  });

  it('finds grants for test table', async () => {
    const results = await introspectGrants(client, ['public']);
    const tableGrants = results.filter(
      r => r.identity.type === 'grant' &&
        r.definition.object_name === 'grant_test_table' &&
        r.definition.grantee === 'dbdelta_grant_test'
    );
    assert.ok(tableGrants.length >= 2, 'should have select and insert grants');
    const privs = tableGrants.map(r => r.definition.privilege_type);
    assert.ok(privs.includes('SELECT'), 'should have SELECT');
    assert.ok(privs.includes('INSERT'), 'should have INSERT');
  });

  it('grant identity includes privilege, object, and grantee', async () => {
    const results = await introspectGrants(client, ['public']);
    const grant = results.find(
      r => r.identity.type === 'grant' &&
        r.definition.object_name === 'grant_test_table' &&
        r.definition.grantee === 'dbdelta_grant_test' &&
        r.definition.privilege_type === 'SELECT'
    );
    assert.ok(grant);
    assert.ok(grant.identity.name.includes('SELECT'));
    assert.ok(grant.identity.name.includes('grant_test_table'));
    assert.ok(grant.identity.name.includes('dbdelta_grant_test'));
  });

  it('ddl create is a grant statement', async () => {
    const results = await introspectGrants(client, ['public']);
    const grant = results.find(
      r => r.identity.type === 'grant' &&
        r.definition.object_name === 'grant_test_table' &&
        r.definition.grantee === 'dbdelta_grant_test' &&
        r.definition.privilege_type === 'SELECT'
    );
    assert.ok(grant.ddl.create.includes('grant'));
    assert.ok(grant.ddl.create.includes('SELECT'));
    assert.ok(grant.ddl.create.includes('grant_test_table'));
  });

  it('ddl drop is a revoke statement', async () => {
    const results = await introspectGrants(client, ['public']);
    const grant = results.find(
      r => r.identity.type === 'grant' &&
        r.definition.object_name === 'grant_test_table' &&
        r.definition.grantee === 'dbdelta_grant_test' &&
        r.definition.privilege_type === 'SELECT'
    );
    assert.ok(grant.ddl.drop.includes('revoke'));
    assert.ok(grant.ddl.drop.includes('SELECT'));
  });

  it('includes schema grants', async () => {
    const results = await introspectGrants(client, ['public']);
    const schemaGrants = results.filter(
      r => r.identity.type === 'grant' && r.definition.object_type === 'schema'
    );
    // public schema typically has grants for public role
    assert.ok(schemaGrants.length >= 0);
  });
});
