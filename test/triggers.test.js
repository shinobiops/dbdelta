import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { introspectTriggers } from '../src/introspector/triggers.js';
import { setupTestDatabases, teardownTestDatabases, getTestUrls, connectTo } from './helpers/db.js';

describe('introspectTriggers', () => {
  let client;

  before(async () => {
    await setupTestDatabases();
    const { fromUrl } = getTestUrls();
    client = await connectTo(fromUrl);

    await client.query(`create table public.audit_log (id serial, action text, ts timestamptz default now())`);
    await client.query(`create table public.items (id serial primary key, name text, updated_at timestamptz)`);

    await client.query(`
      create function public.log_update()
      returns trigger
      language plpgsql
      as $$
      begin
        insert into public.audit_log (action) values ('update');
        return new;
      end;
      $$
    `);

    await client.query(`
      create trigger items_update_trigger
      after update on public.items
      for each row
      execute function public.log_update()
    `);
  });

  after(async () => {
    if (client) await client.end();
    await teardownTestDatabases();
  });

  it('returns triggers', async () => {
    const results = await introspectTriggers(client, ['public']);
    const names = results.map(r => r.definition.name);
    assert.ok(names.includes('items_update_trigger'), 'should find items_update_trigger');
  });

  it('has correct identity type', async () => {
    const results = await introspectTriggers(client, ['public']);
    const trig = results.find(r => r.definition.name === 'items_update_trigger');
    assert.equal(trig.identity.type, 'trigger');
  });

  it('identity name includes table name', async () => {
    const results = await introspectTriggers(client, ['public']);
    const trig = results.find(r => r.definition.name === 'items_update_trigger');
    assert.equal(trig.identity.name, 'items_update_trigger.on.items');
  });

  it('captures trigger properties', async () => {
    const results = await introspectTriggers(client, ['public']);
    const trig = results.find(r => r.definition.name === 'items_update_trigger');
    assert.equal(trig.definition.table_name, 'items');
    assert.equal(trig.definition.function_name, 'log_update');
  });

  it('ddl create includes trigger definition', async () => {
    const results = await introspectTriggers(client, ['public']);
    const trig = results.find(r => r.definition.name === 'items_update_trigger');
    assert.ok(trig.ddl.create.includes('items_update_trigger'));
    assert.ok(trig.ddl.create.includes('items'));
  });

  it('ddl drop includes drop trigger', async () => {
    const results = await introspectTriggers(client, ['public']);
    const trig = results.find(r => r.definition.name === 'items_update_trigger');
    assert.ok(trig.ddl.drop.includes('drop trigger'));
    assert.ok(trig.ddl.drop.includes('items_update_trigger'));
  });

  it('excludes internal triggers', async () => {
    const results = await introspectTriggers(client, ['public']);
    for (const r of results) {
      // internal triggers typically start with RI_ for FK constraints
      assert.ok(!r.definition.name.startsWith('RI_'), 'should not include internal triggers');
    }
  });
});
