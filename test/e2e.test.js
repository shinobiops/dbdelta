import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';

const PG_URL = process.env.TEST_PG_URL || 'postgres://postgres:postgres@localhost:5433/postgres';

describe('end-to-end pipeline', () => {
  let fromClient;
  let toClient;

  before(async () => {
    // Use a single PG server with two schemas to simulate two databases
    fromClient = new pg.Client({ connectionString: PG_URL });
    toClient = new pg.Client({ connectionString: PG_URL });
    await Promise.all([fromClient.connect(), toClient.connect()]);

    // Setup "from" schema
    await fromClient.query(`
      drop schema if exists e2e_from cascade;
      create schema e2e_from;

      create table e2e_from.users (
        id serial primary key,
        name text not null,
        email text
      );

      create index idx_users_email on e2e_from.users (email);

      create table e2e_from.posts (
        id serial primary key,
        user_id integer references e2e_from.users(id),
        title text,
        body text
      );

      create view e2e_from.active_users as
        select id, name from e2e_from.users where name is not null;

      create table e2e_from.old_table (
        id serial primary key
      );

      create or replace function e2e_from.hello() returns text
        language sql as $$ select 'hello'::text $$;
    `);

    // Setup "to" schema (target state)
    await toClient.query(`
      drop schema if exists e2e_to cascade;
      create schema e2e_to;

      create table e2e_to.users (
        id serial primary key,
        name text not null,
        email text not null,
        created_at timestamp default now()
      );

      create index idx_users_email on e2e_to.users (email);
      create index idx_users_name on e2e_to.users (name);

      create table e2e_to.posts (
        id serial primary key,
        user_id integer references e2e_to.users(id),
        title text not null,
        body text,
        published boolean default false
      );

      create view e2e_to.active_users as
        select id, name, email from e2e_to.users where name is not null;

      create table e2e_to.comments (
        id serial primary key,
        post_id integer references e2e_to.posts(id),
        body text
      );

      create or replace function e2e_to.hello() returns text
        language sql as $$ select 'hello world'::text $$;
    `);
  });

  after(async () => {
    if (fromClient) {
      await fromClient.query('drop schema if exists e2e_from cascade;');
      await fromClient.end();
    }
    if (toClient) {
      await toClient.query('drop schema if exists e2e_to cascade;');
      await toClient.end();
    }
  });

  it('runs the full pipeline and produces valid SQL', async () => {
    const { introspectDb } = await import('../src/introspector/index.js');
    const { buildDependencyInfo } = await import('../src/dependencies.js');
    const { diff } = await import('../src/differ.js');
    const { generate } = await import('../src/sql-generator.js');

    // Introspect the "from" and "to" schemas separately
    const fromObjects = await introspectDb(fromClient, ['e2e_from']);
    const toObjects = await introspectDb(toClient, ['e2e_to']);

    // Remap to a common schema for comparison
    // (In real usage, both DBs would use the same schema names)
    const remapSchema = (objects, fromSchema, toSchema) =>
      objects.map(obj => ({
        ...obj,
        identity: { ...obj.identity, schema: toSchema },
        definition: obj.definition.schema === fromSchema
          ? { ...obj.definition, schema: toSchema }
          : obj.definition,
      }));

    const fromRemapped = remapSchema(fromObjects, 'e2e_from', 'e2e_test');
    const toRemapped = remapSchema(toObjects, 'e2e_to', 'e2e_test');

    const depInfo = await buildDependencyInfo(fromClient, toClient, ['e2e_from', 'e2e_to']);
    // Override with empty sorted arrays since schema remapping breaks key matching
    depInfo.sortedCreates = [];
    depInfo.sortedDrops = [];

    const operations = diff(fromRemapped, toRemapped, {});

    assert.ok(operations.length > 0, 'should have change operations');

    const sql = generate(operations, depInfo);

    // Verify structure
    assert.ok(sql.includes('dbdelta migration'), 'should have header');
    assert.ok(sql.includes('PHASE 1'), 'should have phase 1');
    assert.ok(sql.includes('PHASE 2'), 'should have phase 2');
    assert.ok(sql.includes('PHASE 3'), 'should have phase 3');

    // Verify drops are commented out
    // old_table should be dropped (commented)
    assert.ok(sql.includes('-- drop'), 'drops should be commented out');

    // Verify new objects are created
    assert.ok(sql.includes('comments'), 'should reference comments table');

    // Verify the view is recreated (active_users changed)
    assert.ok(sql.includes('active_users'), 'should update active_users view');

    // Verify new index
    assert.ok(sql.includes('idx_users_name'), 'should create idx_users_name');

    // Verify column additions
    assert.ok(sql.includes('created_at'), 'should add created_at column');
    assert.ok(sql.includes('published'), 'should add published column');

    // Verify phase ordering: PHASE 1 before PHASE 2 before PHASE 3
    const p1 = sql.indexOf('PHASE 1');
    const p2 = sql.indexOf('PHASE 2');
    const p3 = sql.indexOf('PHASE 3');
    assert.ok(p1 < p2, 'phase 1 before phase 2');
    assert.ok(p2 < p3, 'phase 2 before phase 3');
  });

  it('produces expected operation types', async () => {
    const { introspectDb } = await import('../src/introspector/index.js');
    const { diff } = await import('../src/differ.js');

    const fromObjects = await introspectDb(fromClient, ['e2e_from']);
    const toObjects = await introspectDb(toClient, ['e2e_to']);

    const remapSchema = (objects, fromSchema, toSchema) =>
      objects.map(obj => ({
        ...obj,
        identity: { ...obj.identity, schema: toSchema },
        definition: obj.definition.schema === fromSchema
          ? { ...obj.definition, schema: toSchema }
          : obj.definition,
      }));

    const fromRemapped = remapSchema(fromObjects, 'e2e_from', 'e2e_test');
    const toRemapped = remapSchema(toObjects, 'e2e_to', 'e2e_test');

    const operations = diff(fromRemapped, toRemapped, {});

    const opTypes = new Set(operations.map(o => o.op));

    // Should have CREATE ops (new table, new columns, new index)
    assert.ok(opTypes.has('CREATE'), 'should have CREATE operations');

    // Should have DROP ops (old_table)
    assert.ok(opTypes.has('DROP'), 'should have DROP operations');

    // Should have some kind of change ops for modified objects
    const changeOps = operations.filter(o =>
      o.op === 'ALTER' || o.op === 'CREATE_OR_REPLACE' || o.op === 'DROP_AND_CREATE'
    );
    assert.ok(changeOps.length > 0, 'should have change operations for modified objects');
  });
});
