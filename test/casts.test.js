import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { introspectCasts } from '../src/introspector/casts.js';
import { setupTestDatabases, teardownTestDatabases, getTestUrls, connectTo } from './helpers/db.js';

describe('introspectCasts', () => {
  let client;

  before(async () => {
    await setupTestDatabases();
    const { fromUrl } = getTestUrls();
    client = await connectTo(fromUrl);

    // Create a custom type and cast function
    await client.query(`create domain public.posint as integer check (value > 0)`);

    await client.query(`
      create function public.posint_to_text(public.posint)
      returns text
      language sql
      immutable
      as $$ select $1::integer::text $$
    `);

    await client.query(`
      create cast (public.posint as text)
      with function public.posint_to_text(public.posint)
      as assignment
    `);
  });

  after(async () => {
    if (client) await client.end();
    await teardownTestDatabases();
  });

  it('returns casts', async () => {
    const results = await introspectCasts(client, ['public']);
    assert.ok(results.length > 0, 'should find at least one cast');
  });

  it('finds custom cast', async () => {
    const results = await introspectCasts(client, ['public']);
    const cast = results.find(r => r.identity.name.includes('posint') && r.identity.name.includes('text'));
    assert.ok(cast, 'should find posint::text cast');
  });

  it('has correct identity type', async () => {
    const results = await introspectCasts(client, ['public']);
    const cast = results.find(r => r.identity.name.includes('posint'));
    assert.equal(cast.identity.type, 'cast');
  });

  it('captures cast properties', async () => {
    const results = await introspectCasts(client, ['public']);
    const cast = results.find(r => r.identity.name.includes('posint') && r.identity.name.includes('text'));
    assert.equal(cast.definition.cast_context, 'assignment');
    assert.equal(cast.definition.cast_method, 'f');
    assert.ok(cast.definition.cast_func.includes('posint_to_text'));
  });

  it('ddl create includes cast definition', async () => {
    const results = await introspectCasts(client, ['public']);
    const cast = results.find(r => r.identity.name.includes('posint') && r.identity.name.includes('text'));
    assert.ok(cast.ddl.create.includes('create cast'));
    assert.ok(cast.ddl.create.includes('posint'));
    assert.ok(cast.ddl.create.includes('as assignment'));
  });

  it('ddl drop includes type signature', async () => {
    const results = await introspectCasts(client, ['public']);
    const cast = results.find(r => r.identity.name.includes('posint') && r.identity.name.includes('text'));
    assert.ok(cast.ddl.drop.includes('posint'));
  });
});
