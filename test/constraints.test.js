import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { introspectConstraints } from '../src/introspector/constraints.js';
import { setupTestDatabases, teardownTestDatabases, getTestUrls, connectTo } from './helpers/db.js';

describe('introspectConstraints', () => {
  let client;

  before(async () => {
    await setupTestDatabases();
    const { fromUrl } = getTestUrls();
    client = await connectTo(fromUrl);

    await client.query(`
      create table public.parent_tbl (
        id integer primary key
      )
    `);
    await client.query(`
      create table public.child_tbl (
        id integer primary key,
        parent_id integer references public.parent_tbl(id),
        email text unique,
        age integer constraint age_check check (age > 0)
      )
    `);
  });

  after(async () => {
    if (client) await client.end();
    await teardownTestDatabases();
  });

  it('detects primary key constraints', async () => {
    const results = await introspectConstraints(client, ['public']);
    const pks = results.filter(r => r.definition.constraint_type === 'primary_key');
    assert.ok(pks.length >= 2, 'should find primary keys');
  });

  it('detects foreign key constraints', async () => {
    const results = await introspectConstraints(client, ['public']);
    const fks = results.filter(r => r.definition.constraint_type === 'foreign_key');
    assert.ok(fks.length >= 1, 'should find foreign key');
    const fk = fks.find(f => f.definition.table === 'child_tbl');
    assert.ok(fk.definition.definition.includes('REFERENCES'));
  });

  it('detects unique constraints', async () => {
    const results = await introspectConstraints(client, ['public']);
    const uniques = results.filter(r => r.definition.constraint_type === 'unique');
    assert.ok(uniques.length >= 1, 'should find unique constraint');
  });

  it('detects check constraints', async () => {
    const results = await introspectConstraints(client, ['public']);
    const checks = results.filter(r => r.definition.constraint_type === 'check');
    const ageCheck = checks.find(c => c.definition.name === 'age_check');
    assert.ok(ageCheck, 'should find age_check');
    assert.ok(ageCheck.definition.definition.includes('age > 0'));
  });

  it('uses table.constraint_name as identity name', async () => {
    const results = await introspectConstraints(client, ['public']);
    const ageCheck = results.find(r => r.definition.name === 'age_check');
    assert.equal(ageCheck.identity.name, 'child_tbl.age_check');
    assert.equal(ageCheck.identity.type, 'constraint');
  });

  it('ddl create includes alter table add constraint', async () => {
    const results = await introspectConstraints(client, ['public']);
    const ageCheck = results.find(r => r.definition.name === 'age_check');
    assert.ok(ageCheck.ddl.create.includes('alter table'));
    assert.ok(ageCheck.ddl.create.includes('add constraint'));
  });
});
