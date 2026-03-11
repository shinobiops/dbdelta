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
        identity: { schema: 'public', type: 'grant', name: 'select_on_t1' },
        toDef: {},
        ddl: makeDdl('grant select on public.t1 to reader;', null, null),
        reason: 'new',
      },
    ];
    const depInfo = { sortedCreates: ['public.table.t1'], sortedDrops: [], fromGraph: new Map(), toGraph: new Map() };
    const sql = generate(ops, depInfo);
    const phase2Idx = sql.indexOf('PHASE 2');
    const phase3Idx = sql.indexOf('PHASE 3');
    const createIdx = sql.indexOf('create table');
    const grantIdx = sql.indexOf('grant select');
    assert.ok(createIdx > phase2Idx);
    assert.ok(grantIdx > phase3Idx);
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
