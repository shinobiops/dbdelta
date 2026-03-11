import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { introspectTypes } from '../src/introspector/types.js';
import { setupTestDatabases, teardownTestDatabases, getTestUrls, connectTo } from './helpers/db.js';

describe('introspectTypes', () => {
  let client;

  before(async () => {
    await setupTestDatabases();
    const { fromUrl } = getTestUrls();
    client = await connectTo(fromUrl);

    // Create enum type
    await client.query(`create type public.status_enum as enum ('active', 'inactive', 'pending')`);

    // Create composite type
    await client.query(`create type public.address_type as (street text, city text, zip text)`);

    // Create domain type
    await client.query(`create domain public.positive_int as integer check (value > 0) not null default 1`);

    // Create range type
    await client.query(`create type public.float_range as range (subtype = float8)`);
  });

  after(async () => {
    if (client) await client.end();
    await teardownTestDatabases();
  });

  it('introspects enum types', async () => {
    const results = await introspectTypes(client, ['public']);
    const enums = results.filter(r => r.identity.type === 'type_enum');
    const statusEnum = enums.find(e => e.identity.name === 'status_enum');
    assert.ok(statusEnum, 'should find status_enum');
    assert.deepEqual(statusEnum.definition.labels, ['active', 'inactive', 'pending']);
    assert.ok(statusEnum.ddl.create.includes("'active'"));
    assert.ok(statusEnum.ddl.create.includes('create type'));
  });

  it('enum alter generates add value', async () => {
    const results = await introspectTypes(client, ['public']);
    const statusEnum = results.find(e => e.identity.name === 'status_enum');
    const fromDef = { ...statusEnum.definition, labels: ['active', 'inactive'] };
    const toDef = statusEnum.definition;
    const alterSql = statusEnum.ddl.alter(fromDef, toDef);
    assert.ok(alterSql.includes("add value 'pending'"));
  });

  it('introspects composite types', async () => {
    const results = await introspectTypes(client, ['public']);
    const composites = results.filter(r => r.identity.type === 'type_composite');
    const addr = composites.find(c => c.identity.name === 'address_type');
    assert.ok(addr, 'should find address_type');
    assert.equal(addr.definition.attributes.length, 3);
    assert.equal(addr.definition.attributes[0].name, 'street');
    assert.equal(addr.definition.attributes[0].type, 'text');
    assert.ok(addr.ddl.create.includes('create type'));
  });

  it('composite alter generates add/alter/drop attribute', async () => {
    const results = await introspectTypes(client, ['public']);
    const addr = results.find(c => c.identity.name === 'address_type');
    const fromDef = {
      ...addr.definition,
      attributes: [
        { name: 'street', type: 'text' },
        { name: 'city', type: 'integer' },
        { name: 'old_field', type: 'text' },
      ],
    };
    const toDef = addr.definition;
    const alterSql = addr.ddl.alter(fromDef, toDef);
    assert.ok(alterSql.includes('add attribute zip'));
    assert.ok(alterSql.includes('alter attribute city'));
    assert.ok(alterSql.includes('drop attribute old_field'));
  });

  it('introspects domain types', async () => {
    const results = await introspectTypes(client, ['public']);
    const domains = results.filter(r => r.identity.type === 'type_domain');
    const posInt = domains.find(d => d.identity.name === 'positive_int');
    assert.ok(posInt, 'should find positive_int');
    assert.equal(posInt.definition.base_type, 'integer');
    assert.equal(posInt.definition.not_null, true);
    assert.ok(posInt.definition.constraints.length > 0);
    assert.ok(posInt.ddl.create.includes('create domain'));
  });

  it('introspects range types', async () => {
    const results = await introspectTypes(client, ['public']);
    const ranges = results.filter(r => r.identity.type === 'type_range');
    const floatRange = ranges.find(r => r.identity.name === 'float_range');
    assert.ok(floatRange, 'should find float_range');
    assert.equal(floatRange.definition.subtype, 'double precision');
    assert.ok(floatRange.ddl.create.includes('create type'));
    assert.ok(floatRange.ddl.create.includes('subtype = double precision'));
  });
});
