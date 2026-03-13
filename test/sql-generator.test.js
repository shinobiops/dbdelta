import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { generate, commentOut, formatHeader } from '../src/sql-generator.js';

describe('commentOut', () => {
  it('comments out single line', () => {
    assert.equal(commentOut('drop table public.users;'), '-- drop table public.users;');
  });

  it('comments out multiple lines', () => {
    const input = 'drop table public.users;\ndrop table public.posts;';
    const expected = '-- drop table public.users;\n-- drop table public.posts;';
    assert.equal(commentOut(input), expected);
  });

  it('handles empty string', () => {
    assert.equal(commentOut(''), '-- ');
  });
});

describe('formatHeader', () => {
  it('includes generation timestamp', () => {
    const header = formatHeader();
    assert.ok(header.includes('dbdelta migration'));
    assert.ok(header.includes('Generated:'));
    assert.ok(header.includes('WARNING'));
  });
});

describe('generate', () => {
  // Helper that creates ddl objects with both string and function forms
  const makeDdl = (createSql, dropSql, alterSql) => ({
    create: createSql,
    drop: dropSql,
    alter: typeof alterSql === 'string' ? () => alterSql : alterSql,
  });

  it('produces header and phase markers', () => {
    const sql = generate([], { sortedCreates: [], sortedDrops: [], fromGraph: new Map(), toGraph: new Map() });
    assert.ok(sql.includes('dbdelta migration'));
    assert.ok(sql.includes('PHASE 1'));
    assert.ok(sql.includes('PHASE 2'));
    assert.ok(sql.includes('PHASE 3'));
  });

  it('comments out DROP operations', () => {
    const ops = [{
      op: 'DROP',
      identity: { schema: 'public', type: 'table', name: 'users' },
      ddl: makeDdl(null, 'drop table public.users;', null),
      reason: 'removed',
    }];
    const depInfo = { sortedCreates: [], sortedDrops: ['public.table.users'], fromGraph: new Map(), toGraph: new Map() };
    const sql = generate(ops, depInfo);
    assert.ok(sql.includes('-- drop table public.users;'));
  });

  it('emits CREATE operations without comments', () => {
    const ops = [{
      op: 'CREATE',
      identity: { schema: 'public', type: 'table', name: 'users' },
      toDef: {},
      ddl: makeDdl('create table public.users (\n  id serial primary key\n);', null, null),
      reason: 'new object',
    }];
    const depInfo = { sortedCreates: ['public.table.users'], sortedDrops: [], fromGraph: new Map(), toGraph: new Map() };
    const sql = generate(ops, depInfo);
    assert.ok(sql.includes('create table public.users'));
    // The SQL itself should NOT be commented out
    assert.ok(sql.includes('\ncreate table'));
  });

  it('emits ALTER operations', () => {
    const ops = [{
      op: 'ALTER',
      identity: { schema: 'public', type: 'table', name: 'users' },
      fromDef: {},
      toDef: {},
      ddl: makeDdl(null, null, 'alter table public.users add column email text;'),
      reason: 'definition changed',
    }];
    const depInfo = { sortedCreates: ['public.table.users'], sortedDrops: [], fromGraph: new Map(), toGraph: new Map() };
    const sql = generate(ops, depInfo);
    assert.ok(sql.includes('alter table public.users add column email text;'));
  });

  it('emits CREATE OR REPLACE operations', () => {
    const ops = [{
      op: 'CREATE_OR_REPLACE',
      identity: { schema: 'public', type: 'function', name: 'get_user' },
      toDef: {},
      ddl: {
        createOrReplace: 'create or replace function public.get_user() returns void as $$ begin end; $$ language plpgsql;',
      },
      reason: 'definition changed',
    }];
    const depInfo = { sortedCreates: ['public.function.get_user'], sortedDrops: [], fromGraph: new Map(), toGraph: new Map() };
    const sql = generate(ops, depInfo);
    assert.ok(sql.includes('create or replace function'));
  });

  it('emits DROP_AND_CREATE as drop then create', () => {
    const ops = [{
      op: 'DROP_AND_CREATE',
      identity: { schema: 'public', type: 'index', name: 'idx_users_email' },
      fromDef: {},
      toDef: {},
      ddl: makeDdl(
        'create index idx_users_email on public.users (email);',
        'drop index public.idx_users_email;',
        null
      ),
      reason: 'definition changed',
    }];
    const depInfo = {
      sortedCreates: ['public.index.idx_users_email'],
      sortedDrops: ['public.index.idx_users_email'],
      fromGraph: new Map(),
      toGraph: new Map(),
    };
    const sql = generate(ops, depInfo);
    assert.ok(sql.includes('drop index'));
    assert.ok(sql.includes('create index'));
  });

  it('emits simple drop+create for enum without dependents', () => {
    const ops = [{
      op: 'DROP_AND_CREATE',
      identity: { schema: 'app', type: 'enum', name: 'status' },
      fromDef: { labels: ['a', 'b', 'c'] },
      toDef: { labels: ['a', 'b'] },
      ddl: makeDdl(
        "create type app.status as enum ('a', 'b');",
        'drop type app.status;',
        null
      ),
      reason: 'definition changed',
    }];
    const depInfo = { sortedCreates: [], sortedDrops: [], fromGraph: new Map(), toGraph: new Map() };
    const sql = generate(ops, depInfo);
    assert.ok(sql.includes('drop type app.status;'));
    assert.ok(sql.includes("create type app.status as enum ('a', 'b');"));
    assert.ok(!sql.includes('__dbdelta_new'), 'should not use safe swap without dependents');
  });

  it('emits safe swap for enum DROP_AND_CREATE with dependents', () => {
    const ops = [{
      op: 'DROP_AND_CREATE',
      identity: { schema: 'app', type: 'enum', name: 'status' },
      fromDef: { labels: ['a', 'b', 'c'] },
      toDef: { labels: ['a', 'b'] },
      ddl: makeDdl(
        "create type app.status as enum ('a', 'b');",
        'drop type app.status;',
        null
      ),
      reason: 'definition changed',
    }];
    const fromObjects = [
      {
        identity: { schema: 'app', type: 'column', name: 'tasks.status' },
        definition: { name: 'status', table: 'tasks', schema: 'app', data_type: 'app.status' },
      },
    ];
    // Graph: app.table.tasks depends on app.type.status
    const fromGraph = new Map([
      ['app.table.tasks', new Set(['app.type.status'])],
      ['app.type.status', new Set()],
    ]);
    const depInfo = { sortedCreates: [], sortedDrops: [], fromGraph, toGraph: new Map() };
    const sql = generate(ops, depInfo, fromObjects);
    assert.ok(sql.includes('app.__dbdelta_new_status'), 'should create temp type');
    assert.ok(sql.includes('alter table app.tasks alter column status set data type app.__dbdelta_new_status'), 'should alter column');
    assert.ok(sql.includes('drop type app.status;'), 'should drop old type');
    assert.ok(sql.includes('alter type app.__dbdelta_new_status rename to status;'), 'should rename');
  });

  it('emits simple drop+create for function without dependents', () => {
    const ops = [{
      op: 'DROP_AND_CREATE',
      identity: { schema: 'app', type: 'function', name: 'get_user(uuid)' },
      fromDef: { result_type: 'void', identity_args: 'uuid' },
      toDef: { result_type: 'TABLE(id uuid)', identity_args: 'uuid' },
      ddl: {
        create: 'create function app.get_user(uuid) returns table(id uuid) as $$ begin end; $$ language plpgsql;',
        createOrReplace: 'create or replace function app.get_user(uuid) returns table(id uuid) as $$ begin end; $$ language plpgsql;',
        drop: 'drop function app.get_user(uuid);',
      },
      reason: 'definition changed',
    }];
    const depInfo = { sortedCreates: [], sortedDrops: [], fromGraph: new Map(), toGraph: new Map() };
    const sql = generate(ops, depInfo);
    assert.ok(sql.includes('drop function app.get_user(uuid);'));
    assert.ok(sql.includes('create or replace function app.get_user'));
    assert.ok(!sql.includes('__dbdelta_old'), 'should not use safe swap without dependents');
  });

  it('emits safe swap for function DROP_AND_CREATE with dependents', () => {
    const ops = [{
      op: 'DROP_AND_CREATE',
      identity: { schema: 'app', type: 'function', name: 'get_user(uuid)' },
      fromDef: { result_type: 'void', identity_args: 'uuid' },
      toDef: { result_type: 'TABLE(id uuid)', identity_args: 'uuid' },
      ddl: {
        create: 'create function app.get_user(uuid) returns table(id uuid) as $$ begin end; $$ language plpgsql;',
        createOrReplace: 'create or replace function app.get_user(uuid) returns table(id uuid) as $$ begin end; $$ language plpgsql;',
        drop: 'drop function app.get_user(uuid);',
      },
      reason: 'definition changed',
    }];
    // Graph: app.view.user_view depends on app.function.get_user
    const fromGraph = new Map([
      ['app.view.user_view', new Set(['app.function.get_user'])],
      ['app.function.get_user', new Set()],
    ]);
    const depInfo = { sortedCreates: [], sortedDrops: [], fromGraph, toGraph: new Map() };
    const sql = generate(ops, depInfo);
    assert.ok(sql.includes('alter function app.get_user(uuid) rename to __dbdelta_old_get_user;'), 'should rename old');
    assert.ok(sql.includes('create or replace function app.get_user'), 'should create new');
    assert.ok(sql.includes('drop function app.__dbdelta_old_get_user(uuid);'), 'should drop old renamed');
  });

  it('emits RENAME operations', () => {
    const ops = [{
      op: 'RENAME',
      identity: { schema: 'public', type: 'table', name: 'users' },
      newName: 'accounts',
      ddl: { rename: (oldName, newName) => `alter table public.${oldName} rename to ${newName};` },
      reason: 'rename',
    }];
    const depInfo = { sortedCreates: [], sortedDrops: [], fromGraph: new Map(), toGraph: new Map() };
    const sql = generate(ops, depInfo);
    assert.ok(sql.includes('alter table public.users rename to accounts;'));
  });

  it('separates grants into phase 3', () => {
    const ops = [
      {
        op: 'CREATE',
        identity: { schema: 'public', type: 'table', name: 't1' },
        toDef: {},
        ddl: makeDdl('create table public.t1 (id integer);', null, null),
        reason: 'new',
      },
      {
        op: 'CREATE',
        identity: { schema: 'public', type: 'grant', name: 'SELECT/public.t1/reader' },
        toDef: {
          privilege_type: 'SELECT',
          object_type: 'table',
          schema: 'public',
          object_name: 't1',
          grantee: 'reader',
          is_grantable: false,
        },
        ddl: makeDdl('grant SELECT on table public.t1 to reader;', null, null),
        reason: 'new',
      },
    ];
    const depInfo = { sortedCreates: ['public.table.t1'], sortedDrops: [], fromGraph: new Map(), toGraph: new Map() };
    const sql = generate(ops, depInfo);
    const phase2Idx = sql.indexOf('PHASE 2');
    const phase3Idx = sql.indexOf('PHASE 3');
    const createIdx = sql.indexOf('create table');
    const grantIdx = sql.indexOf('grant');
    assert.ok(createIdx > phase2Idx);
    assert.ok(grantIdx > phase3Idx);
  });

  it('combines grants by object and grantee', () => {
    const makeGrantOp = (priv, grantee) => ({
      op: 'CREATE',
      identity: { schema: 'app', type: 'grant', name: `${priv}/app.t1/${grantee}` },
      toDef: {
        privilege_type: priv,
        object_type: 'table',
        schema: 'app',
        object_name: 't1',
        grantee,
        is_grantable: false,
      },
      ddl: { create: `grant ${priv} on table app.t1 to ${grantee};` },
      reason: 'new',
    });
    const ops = [
      makeGrantOp('SELECT', 'reader'),
      makeGrantOp('INSERT', 'reader'),
      makeGrantOp('UPDATE', 'reader'),
      makeGrantOp('SELECT', 'writer'),
    ];
    const depInfo = { sortedCreates: [], sortedDrops: [], fromGraph: new Map(), toGraph: new Map() };
    const sql = generate(ops, depInfo);
    // reader should get combined privileges
    assert.ok(sql.includes('grant INSERT, SELECT, UPDATE on table app.t1 to reader;'), 'should combine reader grants');
    // writer should get single privilege
    assert.ok(sql.includes('grant SELECT on table app.t1 to writer;'), 'should emit writer grant');
    // Should NOT have individual reader lines
    assert.ok(!sql.includes('grant SELECT on table app.t1 to reader;'), 'should not have individual reader SELECT');
  });

  it('emits grant all when all privileges present', () => {
    const privs = ['DELETE', 'INSERT', 'MAINTAIN', 'REFERENCES', 'SELECT', 'TRIGGER', 'TRUNCATE', 'UPDATE'];
    const ops = privs.map(priv => ({
      op: 'CREATE',
      identity: { schema: 'app', type: 'grant', name: `${priv}/app.t1/admin` },
      toDef: {
        privilege_type: priv,
        object_type: 'table',
        schema: 'app',
        object_name: 't1',
        grantee: 'admin',
        is_grantable: false,
      },
      ddl: { create: `grant ${priv} on table app.t1 to admin;` },
      reason: 'new',
    }));
    const depInfo = { sortedCreates: [], sortedDrops: [], fromGraph: new Map(), toGraph: new Map() };
    const sql = generate(ops, depInfo);
    assert.ok(sql.includes('grant all on table app.t1 to admin;'), 'should emit grant all');
  });

  it('generates default drop when ddl.drop is missing', () => {
    const ops = [{
      op: 'DROP',
      identity: { schema: 'public', type: 'table', name: 'users' },
      ddl: {},
      reason: 'removed',
    }];
    const depInfo = { sortedCreates: [], sortedDrops: ['public.table.users'], fromGraph: new Map(), toGraph: new Map() };
    const sql = generate(ops, depInfo);
    assert.ok(sql.includes('-- drop table public.users;'));
  });

  it('orders operations by dependency', () => {
    const ops = [
      {
        op: 'CREATE',
        identity: { schema: 'public', type: 'view', name: 'v1' },
        toDef: {},
        ddl: makeDdl('create view public.v1 as select 1;', null, null),
        reason: 'new',
      },
      {
        op: 'CREATE',
        identity: { schema: 'public', type: 'table', name: 't1' },
        toDef: {},
        ddl: makeDdl('create table public.t1 (id integer);', null, null),
        reason: 'new',
      },
    ];
    // t1 should come before v1 per dependency order
    const depInfo = {
      sortedCreates: ['public.table.t1', 'public.view.v1'],
      sortedDrops: [],
      fromGraph: new Map(),
      toGraph: new Map(),
    };
    const sql = generate(ops, depInfo);
    const t1Idx = sql.indexOf('create table public.t1');
    const v1Idx = sql.indexOf('create view public.v1');
    assert.ok(t1Idx < v1Idx, 'table should be created before view');
  });
});
