import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { introspectOperators } from '../src/introspector/operators.js';
import { setupTestDatabases, teardownTestDatabases, getTestUrls, connectTo } from './helpers/db.js';

describe('introspectOperators', () => {
  let client;

  before(async () => {
    await setupTestDatabases();
    const { fromUrl } = getTestUrls();
    client = await connectTo(fromUrl);

    // Need a helper function for the operator
    await client.query(`
      create function public.text_concat_op(a text, b text)
      returns text
      language sql
      immutable
      as $$ select a || b $$
    `);

    await client.query(`
      create operator public.||+ (
        function = public.text_concat_op,
        leftarg = text,
        rightarg = text
      )
    `);
  });

  after(async () => {
    if (client) await client.end();
    await teardownTestDatabases();
  });

  it('returns operators', async () => {
    const results = await introspectOperators(client, ['public']);
    assert.ok(results.length > 0, 'should find at least one operator');
    const op = results.find(r => r.definition.name === '||+');
    assert.ok(op, 'should find ||+ operator');
  });

  it('has correct identity type', async () => {
    const results = await introspectOperators(client, ['public']);
    const op = results.find(r => r.definition.name === '||+');
    assert.equal(op.identity.type, 'operator');
  });

  it('identity name includes types', async () => {
    const results = await introspectOperators(client, ['public']);
    const op = results.find(r => r.definition.name === '||+');
    assert.ok(op.identity.name.includes('text'), 'identity name should include type info');
  });

  it('captures operator properties', async () => {
    const results = await introspectOperators(client, ['public']);
    const op = results.find(r => r.definition.name === '||+');
    assert.equal(op.definition.left_type, 'text');
    assert.equal(op.definition.right_type, 'text');
    assert.equal(op.definition.result_type, 'text');
    assert.equal(op.definition.function_name, 'text_concat_op');
  });

  it('ddl create includes operator definition', async () => {
    const results = await introspectOperators(client, ['public']);
    const op = results.find(r => r.definition.name === '||+');
    assert.ok(op.ddl.create.includes('||+'), 'create should include operator name');
    assert.ok(op.ddl.create.includes('text_concat_op'), 'create should include function');
  });

  it('ddl drop includes type signature', async () => {
    const results = await introspectOperators(client, ['public']);
    const op = results.find(r => r.definition.name === '||+');
    assert.ok(op.ddl.drop.includes('text'), 'drop should include type args');
  });
});
