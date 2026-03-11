import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { introspectSequences } from '../src/introspector/sequences.js';
import { setupTestDatabases, teardownTestDatabases, getTestUrls, connectTo } from './helpers/db.js';

describe('introspectSequences', () => {
  let client;

  before(async () => {
    await setupTestDatabases();
    const { fromUrl } = getTestUrls();
    client = await connectTo(fromUrl);

    await client.query(`create sequence public.my_seq increment by 5 start with 100 minvalue 1 maxvalue 10000 cache 10 no cycle`);
    await client.query(`create sequence public.cycling_seq cycle`);
    await client.query(`create table public.owned_tbl (id integer)`);
    await client.query(`create sequence public.owned_seq owned by public.owned_tbl.id`);
  });

  after(async () => {
    if (client) await client.end();
    await teardownTestDatabases();
  });

  it('returns sequences', async () => {
    const results = await introspectSequences(client, ['public']);
    const names = results.map(r => r.identity.name);
    assert.ok(names.includes('my_seq'), 'should find my_seq');
  });

  it('captures sequence properties', async () => {
    const results = await introspectSequences(client, ['public']);
    const seq = results.find(r => r.identity.name === 'my_seq');
    assert.equal(seq.definition.increment, '5');
    assert.equal(seq.definition.start_value, '100');
    assert.equal(seq.definition.cache_size, '10');
    assert.equal(seq.definition.cycle, false);
  });

  it('detects cycle option', async () => {
    const results = await introspectSequences(client, ['public']);
    const seq = results.find(r => r.identity.name === 'cycling_seq');
    assert.equal(seq.definition.cycle, true);
    assert.ok(seq.ddl.create.includes(' cycle'));
  });

  it('detects owned-by relationship', async () => {
    const results = await introspectSequences(client, ['public']);
    const seq = results.find(r => r.identity.name === 'owned_seq');
    assert.ok(seq, 'should find owned_seq');
    assert.ok(seq.definition.owned_by_column === 'id', 'should be owned by id column');
  });

  it('has correct identity type', async () => {
    const results = await introspectSequences(client, ['public']);
    const seq = results.find(r => r.identity.name === 'my_seq');
    assert.equal(seq.identity.type, 'sequence');
  });

  it('ddl create includes sequence definition', async () => {
    const results = await introspectSequences(client, ['public']);
    const seq = results.find(r => r.identity.name === 'my_seq');
    assert.ok(seq.ddl.create.includes('create sequence'));
    assert.ok(seq.ddl.create.includes('increment by 5'));
    assert.ok(seq.ddl.create.includes('start with 100'));
  });
});
