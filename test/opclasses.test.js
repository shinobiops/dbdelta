import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { introspectOpClasses } from '../src/introspector/opclasses.js';
import { setupTestDatabases, teardownTestDatabases, getTestUrls, connectTo } from './helpers/db.js';

describe('introspectOpClasses', () => {
  let client;

  before(async () => {
    await setupTestDatabases();
    const { fromUrl } = getTestUrls();
    client = await connectTo(fromUrl);

    await client.query(`
      create operator family public.my_fam using hash
    `);

    await client.query(`
      create function public.my_text_hash(text)
      returns integer
      language sql
      immutable
      as $$ select length($1) $$
    `);

    await client.query(`
      create operator class public.my_text_hash_ops
      for type text using hash family public.my_fam as
        operator 1 =,
        function 1 public.my_text_hash(text)
    `);
  });

  after(async () => {
    if (client) await client.end();
    await teardownTestDatabases();
  });

  it('returns operator classes', async () => {
    const results = await introspectOpClasses(client, ['public']);
    assert.ok(results.length > 0, 'should find at least one opclass');
    const oc = results.find(r => r.definition.name === 'my_text_hash_ops');
    assert.ok(oc, 'should find my_text_hash_ops');
  });

  it('has correct identity type', async () => {
    const results = await introspectOpClasses(client, ['public']);
    const oc = results.find(r => r.definition.name === 'my_text_hash_ops');
    assert.equal(oc.identity.type, 'opclass');
  });

  it('captures opclass properties', async () => {
    const results = await introspectOpClasses(client, ['public']);
    const oc = results.find(r => r.definition.name === 'my_text_hash_ops');
    assert.equal(oc.definition.access_method, 'hash');
    assert.equal(oc.definition.input_type, 'text');
  });

  it('ddl create includes opclass definition', async () => {
    const results = await introspectOpClasses(client, ['public']);
    const oc = results.find(r => r.definition.name === 'my_text_hash_ops');
    assert.ok(oc.ddl.create.includes('my_text_hash_ops'));
    assert.ok(oc.ddl.create.includes('hash'));
  });

  it('ddl drop includes access method', async () => {
    const results = await introspectOpClasses(client, ['public']);
    const oc = results.find(r => r.definition.name === 'my_text_hash_ops');
    assert.ok(oc.ddl.drop.includes('hash'));
  });
});
