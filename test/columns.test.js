import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { introspectColumns } from '../src/introspector/columns.js';
import { setupTestDatabases, teardownTestDatabases, getTestUrls, connectTo } from './helpers/db.js';

describe('introspectColumns', () => {
  let client;

  before(async () => {
    await setupTestDatabases();
    const { fromUrl } = getTestUrls();
    client = await connectTo(fromUrl);

    await client.query(`
      create table public.test_cols (
        id integer generated always as identity,
        name text not null,
        email text default 'unknown',
        score integer generated always as (id * 2) stored,
        bio text
      )
    `);
  });

  after(async () => {
    if (client) await client.end();
    await teardownTestDatabases();
  });

  it('returns all columns for a table', async () => {
    const results = await introspectColumns(client, ['public']);
    const cols = results.filter(r => r.definition.table === 'test_cols');
    assert.equal(cols.length, 5);
  });

  it('detects identity columns', async () => {
    const results = await introspectColumns(client, ['public']);
    const idCol = results.find(r => r.definition.name === 'id' && r.definition.table === 'test_cols');
    assert.ok(idCol, 'should find id column');
    assert.equal(idCol.definition.identity, 'a');
    assert.ok(idCol.ddl.create.includes('generated always as identity'));
  });

  it('detects not null', async () => {
    const results = await introspectColumns(client, ['public']);
    const nameCol = results.find(r => r.definition.name === 'name' && r.definition.table === 'test_cols');
    assert.equal(nameCol.definition.not_null, true);
    assert.ok(nameCol.ddl.create.includes('not null'));
  });

  it('detects defaults', async () => {
    const results = await introspectColumns(client, ['public']);
    const emailCol = results.find(r => r.definition.name === 'email' && r.definition.table === 'test_cols');
    assert.ok(emailCol.definition.default, 'should have a default');
    assert.ok(emailCol.ddl.create.includes('default'));
  });

  it('detects generated columns', async () => {
    const results = await introspectColumns(client, ['public']);
    const scoreCol = results.find(r => r.definition.name === 'score' && r.definition.table === 'test_cols');
    assert.equal(scoreCol.definition.generated, 's');
    assert.ok(scoreCol.ddl.create.includes('generated always as'));
  });

  it('uses table.column as identity name', async () => {
    const results = await introspectColumns(client, ['public']);
    const idCol = results.find(r => r.definition.name === 'id' && r.definition.table === 'test_cols');
    assert.equal(idCol.identity.name, 'test_cols.id');
    assert.equal(idCol.identity.type, 'column');
  });

  it('alter generates correct statements', async () => {
    const results = await introspectColumns(client, ['public']);
    const bioCol = results.find(r => r.definition.name === 'bio' && r.definition.table === 'test_cols');
    const fromDef = { ...bioCol.definition, not_null: true, data_type: 'integer' };
    const toDef = bioCol.definition;
    const alterSql = bioCol.ddl.alter(fromDef, toDef);
    assert.ok(alterSql.includes('set data type text'));
    assert.ok(alterSql.includes('drop not null'));
  });
});
