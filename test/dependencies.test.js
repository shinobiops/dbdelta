import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { buildGraph, topologicalSort, reverseTopologicalSort, findDependents } from '../src/dependencies.js';

describe('buildGraph', () => {
  it('builds adjacency list from edges', () => {
    const edges = [
      { fromSchema: 'public', fromType: 'view', fromName: 'active_users',
        toSchema: 'public', toType: 'table', toName: 'users' },
      { fromSchema: 'public', fromType: 'view', fromName: 'active_users',
        toSchema: 'public', toType: 'function', toName: 'is_active' },
    ];
    const graph = buildGraph(edges);
    assert.ok(graph.has('public.view.active_users'));
    assert.ok(graph.get('public.view.active_users').has('public.table.users'));
    assert.ok(graph.get('public.view.active_users').has('public.function.is_active'));
  });

  it('returns empty map for no edges', () => {
    const graph = buildGraph([]);
    assert.equal(graph.size, 0);
  });

  it('creates entries for both sides of an edge', () => {
    const edges = [
      { fromSchema: 'public', fromType: 'view', fromName: 'v1',
        toSchema: 'public', toType: 'table', toName: 't1' },
    ];
    const graph = buildGraph(edges);
    assert.ok(graph.has('public.table.t1'));
    assert.equal(graph.get('public.table.t1').size, 0);
  });
});

describe('topologicalSort', () => {
  it('sorts dependencies before dependents', () => {
    const graph = new Map();
    graph.set('public.view.v1', new Set(['public.table.t1']));
    graph.set('public.table.t1', new Set(['public.type.e1']));
    graph.set('public.type.e1', new Set());
    const sorted = topologicalSort(graph);
    assert.ok(sorted.indexOf('public.type.e1') < sorted.indexOf('public.table.t1'));
    assert.ok(sorted.indexOf('public.table.t1') < sorted.indexOf('public.view.v1'));
  });

  it('throws on circular dependency', () => {
    const graph = new Map();
    graph.set('a', new Set(['b']));
    graph.set('b', new Set(['a']));
    assert.throws(() => topologicalSort(graph), /circular/i);
  });

  it('handles single node with no deps', () => {
    const graph = new Map();
    graph.set('public.table.t1', new Set());
    const sorted = topologicalSort(graph);
    assert.deepEqual(sorted, ['public.table.t1']);
  });

  it('handles disconnected components', () => {
    const graph = new Map();
    graph.set('public.table.a', new Set());
    graph.set('public.table.b', new Set());
    graph.set('public.view.c', new Set(['public.table.a']));
    const sorted = topologicalSort(graph);
    assert.equal(sorted.length, 3);
    assert.ok(sorted.indexOf('public.table.a') < sorted.indexOf('public.view.c'));
  });
});

describe('reverseTopologicalSort', () => {
  it('sorts dependents before dependencies', () => {
    const graph = new Map();
    graph.set('public.view.v1', new Set(['public.table.t1']));
    graph.set('public.table.t1', new Set());
    const sorted = reverseTopologicalSort(graph);
    assert.ok(sorted.indexOf('public.view.v1') < sorted.indexOf('public.table.t1'));
  });
});

describe('findDependents', () => {
  it('finds transitive dependents', () => {
    const graph = new Map();
    graph.set('public.view.v1', new Set(['public.table.t1']));
    graph.set('public.view.v2', new Set(['public.view.v1']));
    graph.set('public.table.t1', new Set());
    const deps = findDependents(graph, 'public.table.t1');
    assert.ok(deps.has('public.view.v1'));
    assert.ok(deps.has('public.view.v2'));
    assert.equal(deps.has('public.table.t1'), false);
  });

  it('returns empty set when no dependents', () => {
    const graph = new Map();
    graph.set('public.table.t1', new Set());
    const deps = findDependents(graph, 'public.table.t1');
    assert.equal(deps.size, 0);
  });

  it('handles diamond dependency pattern', () => {
    const graph = new Map();
    graph.set('public.view.v1', new Set(['public.table.t1']));
    graph.set('public.view.v2', new Set(['public.table.t1']));
    graph.set('public.view.v3', new Set(['public.view.v1', 'public.view.v2']));
    graph.set('public.table.t1', new Set());
    const deps = findDependents(graph, 'public.table.t1');
    assert.ok(deps.has('public.view.v1'));
    assert.ok(deps.has('public.view.v2'));
    assert.ok(deps.has('public.view.v3'));
    assert.equal(deps.size, 3);
  });
});
