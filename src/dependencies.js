/**
 * Build identity key string from schema, type, name.
 */
function depIdentityKey(schema, type, name) {
  return `${schema}.${type}.${name}`;
}

/**
 * Build directed graph from dependency edges.
 * Returns Map<string, Set<string>> where key depends on each value in the set.
 */
export function buildGraph(edges) {
  const graph = new Map();
  for (const e of edges) {
    const from = depIdentityKey(e.fromSchema, e.fromType, e.fromName);
    const to = depIdentityKey(e.toSchema, e.toType, e.toName);
    if (!graph.has(from)) graph.set(from, new Set());
    if (!graph.has(to)) graph.set(to, new Set());
    graph.get(from).add(to);
  }
  return graph;
}

/**
 * Kahn's algorithm topological sort.
 * Returns array ordered so dependencies come before dependents.
 */
export function topologicalSort(graph) {
  // Edge A -> B means "A depends on B", so B must come before A.
  // Reverse the edges to get "B is depended on by A", then topo sort.
  const inDegree = new Map();
  const reverseAdj = new Map(); // B -> [A] means A depends on B

  for (const [node] of graph) {
    inDegree.set(node, 0);
    if (!reverseAdj.has(node)) reverseAdj.set(node, []);
  }

  for (const [node, deps] of graph) {
    for (const dep of deps) {
      // node depends on dep, so dep -> node in reverse
      if (!reverseAdj.has(dep)) reverseAdj.set(dep, []);
      reverseAdj.get(dep).push(node);
      inDegree.set(node, (inDegree.get(node) || 0) + 1);
    }
  }

  const queue = [];
  for (const [node, deg] of inDegree) {
    if (deg === 0) queue.push(node);
  }
  queue.sort(); // deterministic ordering for nodes at same level

  const sorted = [];
  while (queue.length > 0) {
    const node = queue.shift();
    sorted.push(node);
    for (const dependent of (reverseAdj.get(node) || [])) {
      const newDeg = inDegree.get(dependent) - 1;
      inDegree.set(dependent, newDeg);
      if (newDeg === 0) {
        queue.push(dependent);
        queue.sort();
      }
    }
  }

  if (sorted.length !== graph.size) {
    const remaining = [...graph.keys()].filter(k => !sorted.includes(k));
    throw new Error(`Circular dependency detected involving: ${remaining.join(', ')}`);
  }

  return sorted;
}

/**
 * Reverse topological sort -- dependents come before dependencies.
 * Used for ordering drops.
 */
export function reverseTopologicalSort(graph) {
  return topologicalSort(graph).reverse();
}

/**
 * Find all objects that transitively depend on the given identity key.
 * Uses BFS on reverse edges (dependent -> dependency becomes dependency -> dependent).
 */
export function findDependents(graph, targetKey) {
  // Build reverse adjacency: for each edge A -> B (A depends on B),
  // store B -> A (B is depended on by A)
  const reverseAdj = new Map();
  for (const [node, deps] of graph) {
    for (const dep of deps) {
      if (!reverseAdj.has(dep)) reverseAdj.set(dep, new Set());
      reverseAdj.get(dep).add(node);
    }
  }

  const visited = new Set();
  const queue = [targetKey];
  while (queue.length > 0) {
    const current = queue.shift();
    for (const dependent of (reverseAdj.get(current) || [])) {
      if (!visited.has(dependent)) {
        visited.add(dependent);
        queue.push(dependent);
      }
    }
  }

  return visited;
}

const DEPENDENCY_QUERY = `
select
  dns.nspname as dep_schema,
  case dc.relkind
    when 'r' then 'table'
    when 'v' then 'view'
    when 'm' then 'materialized_view'
    when 'i' then 'index'
    when 'S' then 'sequence'
    when 'c' then 'composite_type'
    else
      case
        when dp.oid is not null then 'function'
        when dt.oid is not null then 'type'
        else 'unknown'
      end
  end as dep_type,
  coalesce(dc.relname, dp.proname, dt.typname, '') as dep_name,
  rns.nspname as ref_schema,
  case rc.relkind
    when 'r' then 'table'
    when 'v' then 'view'
    when 'm' then 'materialized_view'
    when 'i' then 'index'
    when 'S' then 'sequence'
    when 'c' then 'composite_type'
    else
      case
        when rp.oid is not null then 'function'
        when rt.oid is not null then 'type'
        else 'unknown'
      end
  end as ref_type,
  coalesce(rc.relname, rp.proname, rt.typname, '') as ref_name
from pg_depend d
left join pg_class dc on d.classid = 'pg_class'::regclass and d.objid = dc.oid
left join pg_proc dp on d.classid = 'pg_proc'::regclass and d.objid = dp.oid
left join pg_type dt on d.classid = 'pg_type'::regclass and d.objid = dt.oid
left join pg_namespace dns on dns.oid = coalesce(dc.relnamespace, dp.pronamespace, dt.typnamespace)
left join pg_class rc on d.refclassid = 'pg_class'::regclass and d.refobjid = rc.oid
left join pg_proc rp on d.refclassid = 'pg_proc'::regclass and d.refobjid = rp.oid
left join pg_type rt on d.refclassid = 'pg_type'::regclass and d.refobjid = rt.oid
left join pg_namespace rns on rns.oid = coalesce(rc.relnamespace, rp.pronamespace, rt.typnamespace)
where d.deptype in ('n', 'a')
  and dns.nspname = any($1)
  and rns.nspname = any($1)
`;

/**
 * Query pg_depend from a database and return normalized edges.
 */
export async function queryDependencies(client, schemas) {
  const { rows } = await client.query(DEPENDENCY_QUERY, [schemas]);
  return rows.filter(r => r.dep_name && r.ref_name && r.dep_type !== 'unknown' && r.ref_type !== 'unknown');
}

/**
 * Build dependency info from both databases.
 * Returns { fromGraph, toGraph, sortedCreates, sortedDrops }.
 */
export async function buildDependencyInfo(fromClient, toClient, schemas) {
  const [fromEdges, toEdges] = await Promise.all([
    queryDependencies(fromClient, schemas),
    queryDependencies(toClient, schemas),
  ]);

  const fromGraph = buildGraph(fromEdges);
  const toGraph = buildGraph(toEdges);

  let sortedCreates = [];
  let sortedDrops = [];

  try {
    sortedCreates = topologicalSort(toGraph);
  } catch {
    // If cycles exist, fall back to empty ordering
    sortedCreates = [...toGraph.keys()];
  }

  try {
    sortedDrops = reverseTopologicalSort(fromGraph);
  } catch {
    sortedDrops = [...fromGraph.keys()];
  }

  return { fromGraph, toGraph, sortedCreates, sortedDrops };
}
