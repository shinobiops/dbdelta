import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { introspectEventTriggers } from '../src/introspector/event-triggers.js';
import { setupTestDatabases, teardownTestDatabases, getTestUrls, connectTo } from './helpers/db.js';

describe('introspectEventTriggers', () => {
  let client;

  before(async () => {
    await setupTestDatabases();
    const { fromUrl } = getTestUrls();
    client = await connectTo(fromUrl);

    await client.query(`
      create function public.test_event_trigger_func()
      returns event_trigger
      language plpgsql
      as $$
      begin
        raise notice 'event trigger fired: %', tg_event;
      end;
      $$
    `);

    await client.query(`
      create event trigger test_ddl_trigger on ddl_command_end
      execute function public.test_event_trigger_func()
    `);
  });

  after(async () => {
    if (client) {
      // Event triggers must be dropped before dropping the database
      try {
        await client.query('drop event trigger if exists test_ddl_trigger');
      } catch { /* ignore */ }
      await client.end();
    }
    await teardownTestDatabases();
  });

  it('returns event triggers', async () => {
    const results = await introspectEventTriggers(client, ['public']);
    const names = results.map(r => r.definition.name);
    assert.ok(names.includes('test_ddl_trigger'), 'should find test_ddl_trigger');
  });

  it('has correct identity type and schema', async () => {
    const results = await introspectEventTriggers(client, ['public']);
    const trig = results.find(r => r.definition.name === 'test_ddl_trigger');
    assert.equal(trig.identity.type, 'event_trigger');
    assert.equal(trig.identity.schema, 'pg_global');
  });

  it('captures event type', async () => {
    const results = await introspectEventTriggers(client, ['public']);
    const trig = results.find(r => r.definition.name === 'test_ddl_trigger');
    assert.equal(trig.definition.event, 'ddl_command_end');
  });

  it('captures function info', async () => {
    const results = await introspectEventTriggers(client, ['public']);
    const trig = results.find(r => r.definition.name === 'test_ddl_trigger');
    assert.equal(trig.definition.function_name, 'test_event_trigger_func');
    assert.equal(trig.definition.function_schema, 'public');
  });

  it('ddl create includes event trigger definition', async () => {
    const results = await introspectEventTriggers(client, ['public']);
    const trig = results.find(r => r.definition.name === 'test_ddl_trigger');
    assert.ok(trig.ddl.create.includes('create event trigger'));
    assert.ok(trig.ddl.create.includes('test_ddl_trigger'));
    assert.ok(trig.ddl.create.includes('ddl_command_end'));
  });

  it('ddl drop includes drop event trigger', async () => {
    const results = await introspectEventTriggers(client, ['public']);
    const trig = results.find(r => r.definition.name === 'test_ddl_trigger');
    assert.ok(trig.ddl.drop.includes('drop event trigger'));
    assert.ok(trig.ddl.drop.includes('test_ddl_trigger'));
  });
});
