import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { identity, identityKey } from '../src/introspector/identity.js';

describe('identity', () => {
  it('creates an identity object', () => {
    const id = identity('public', 'table', 'users');
    assert.deepEqual(id, { schema: 'public', type: 'table', name: 'users' });
  });
});

describe('identityKey', () => {
  it('creates a string key from identity', () => {
    assert.equal(identityKey({ schema: 'public', type: 'table', name: 'users' }), 'public.table.users');
  });

  it('produces different keys for different identities', () => {
    const a = identityKey({ schema: 'public', type: 'table', name: 'users' });
    const b = identityKey({ schema: 'app', type: 'table', name: 'users' });
    assert.notEqual(a, b);
  });
});
