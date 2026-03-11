import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { introspect } from '../src/introspector/index.js';
import { setupTestDatabases, teardownTestDatabases, getTestUrls, connectTo } from './helpers/db.js';

describe('introspect orchestrator', () => {
  let fromUrl, toUrl;

  before(async () => {
    const urls = await setupTestDatabases();
    fromUrl = urls.fromUrl;
    toUrl = urls.toUrl;

    // Set up different schemas in from and to databases
    const fromClient = await connectTo(fromUrl);
    await fromClient.query('create schema if not exists app');
    await fromClient.end();

    const toClient = await connectTo(toUrl);
    await toClient.query('create schema if not exists app');
    await toClient.query('create schema if not exists reporting');
    await toClient.end();
  });

  after(async () => {
    await teardownTestDatabases();
  });

  it('returns a Map of combined results from both databases', async () => {
    const result = await introspect(fromUrl, toUrl, ['public', 'app', 'reporting']);
    assert.ok(result instanceof Map);
    assert.ok(result.size > 0, 'should have some entries');
  });

  it('marks objects that exist in both databases', async () => {
    const result = await introspect(fromUrl, toUrl, ['public', 'app']);
    const publicSchema = result.get('public.schema.public');
    assert.ok(publicSchema, 'public schema should exist');
    assert.ok(publicSchema.fromDef !== null, 'should have fromDef');
    assert.ok(publicSchema.toDef !== null, 'should have toDef');
  });

  it('marks objects that exist only in toUrl', async () => {
    const result = await introspect(fromUrl, toUrl, ['public', 'app', 'reporting']);
    const reportingSchema = result.get('reporting.schema.reporting');
    assert.ok(reportingSchema, 'reporting schema should exist');
    assert.equal(reportingSchema.fromDef, null, 'should not have fromDef');
    assert.ok(reportingSchema.toDef !== null, 'should have toDef');
  });
});
