import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { introspectPolicies } from '../src/introspector/policies.js';
import { setupTestDatabases, teardownTestDatabases, getTestUrls, connectTo } from './helpers/db.js';

describe('introspectPolicies', () => {
  let client;

  before(async () => {
    await setupTestDatabases();
    const { fromUrl } = getTestUrls();
    client = await connectTo(fromUrl);

    await client.query(`create table public.documents (id serial primary key, title text, owner_id integer)`);
    await client.query(`alter table public.documents enable row level security`);

    await client.query(`
      create policy documents_select_policy on public.documents
      for select
      using (owner_id = current_setting('app.user_id')::integer)
    `);

    await client.query(`
      create policy documents_insert_policy on public.documents
      for insert
      with check (owner_id = current_setting('app.user_id')::integer)
    `);
  });

  after(async () => {
    if (client) await client.end();
    await teardownTestDatabases();
  });

  it('returns policies', async () => {
    const results = await introspectPolicies(client, ['public']);
    const names = results.map(r => r.definition.name);
    assert.ok(names.includes('documents_select_policy'), 'should find documents_select_policy');
    assert.ok(names.includes('documents_insert_policy'), 'should find documents_insert_policy');
  });

  it('has correct identity type', async () => {
    const results = await introspectPolicies(client, ['public']);
    const pol = results.find(r => r.definition.name === 'documents_select_policy');
    assert.equal(pol.identity.type, 'policy');
  });

  it('identity name includes table name', async () => {
    const results = await introspectPolicies(client, ['public']);
    const pol = results.find(r => r.definition.name === 'documents_select_policy');
    assert.equal(pol.identity.name, 'documents_select_policy.on.documents');
  });

  it('captures select policy with using clause', async () => {
    const results = await introspectPolicies(client, ['public']);
    const pol = results.find(r => r.definition.name === 'documents_select_policy');
    assert.equal(pol.definition.command, 'select');
    assert.ok(pol.definition.using_expr, 'should have using expression');
    assert.equal(pol.definition.permissive, true);
  });

  it('captures insert policy with check clause', async () => {
    const results = await introspectPolicies(client, ['public']);
    const pol = results.find(r => r.definition.name === 'documents_insert_policy');
    assert.equal(pol.definition.command, 'insert');
    assert.ok(pol.definition.with_check_expr, 'should have with check expression');
  });

  it('ddl create includes create policy', async () => {
    const results = await introspectPolicies(client, ['public']);
    const pol = results.find(r => r.definition.name === 'documents_select_policy');
    assert.ok(pol.ddl.create.includes('create policy'));
    assert.ok(pol.ddl.create.includes('documents_select_policy'));
  });

  it('ddl drop includes drop policy', async () => {
    const results = await introspectPolicies(client, ['public']);
    const pol = results.find(r => r.definition.name === 'documents_select_policy');
    assert.ok(pol.ddl.drop.includes('drop policy'));
  });

  it('alter generates correct statements', async () => {
    const results = await introspectPolicies(client, ['public']);
    const pol = results.find(r => r.definition.name === 'documents_select_policy');
    const fromDef = { ...pol.definition, using_expr: 'true' };
    const toDef = pol.definition;
    const alterSql = pol.ddl.alter(fromDef, toDef);
    assert.ok(alterSql.includes('alter policy'), 'should generate alter statement');
    assert.ok(alterSql.includes('using'), 'should include using clause');
  });
});
