import { connect } from '../connection.js';
import { identityKey } from './identity.js';
import { introspectSchemas } from './schemas.js';
import { introspectExtensions } from './extensions.js';
import { introspectTypes } from './types.js';
import { introspectTables } from './tables.js';
import { introspectColumns } from './columns.js';
import { introspectConstraints } from './constraints.js';
import { introspectIndexes } from './indexes.js';
import { introspectSequences } from './sequences.js';
import { introspectFunctions } from './functions.js';
import { introspectViews } from './views.js';
import { introspectMaterializedViews } from './materialized-views.js';
import { introspectTriggers } from './triggers.js';
import { introspectRules } from './rules.js';
import { introspectPolicies } from './policies.js';
import { introspectOperators } from './operators.js';
import { introspectOpClasses } from './opclasses.js';
import { introspectAggregates } from './aggregates.js';
import { introspectCasts } from './casts.js';
import { introspectCollations } from './collations.js';
import { introspectTextSearch } from './text-search.js';
import { introspectStatistics } from './statistics.js';
import { introspectFdw } from './fdw.js';
import { introspectUserMappings } from './user-mappings.js';
import { introspectPublications } from './publications.js';
import { introspectSubscriptions } from './subscriptions.js';
import { introspectEventTriggers } from './event-triggers.js';
import { introspectRoles } from './roles.js';
import { introspectGrants } from './grants.js';

const introspectors = [
  introspectSchemas,
  introspectExtensions,
  introspectTypes,
  introspectTables,
  introspectColumns,
  introspectConstraints,
  introspectIndexes,
  introspectSequences,
  introspectFunctions,
  introspectViews,
  introspectMaterializedViews,
  introspectTriggers,
  introspectRules,
  introspectPolicies,
  introspectOperators,
  introspectOpClasses,
  introspectAggregates,
  introspectCasts,
  introspectCollations,
  introspectTextSearch,
  introspectStatistics,
  introspectFdw,
  introspectUserMappings,
  introspectPublications,
  introspectSubscriptions,
  introspectEventTriggers,
  introspectRoles,
  introspectGrants,
];

export async function introspect(fromUrl, toUrl, schemas) {
  const fromClient = await connect(fromUrl);
  const toClient = await connect(toUrl);

  try {
    const [fromResults, toResults] = await Promise.all([
      runAll(fromClient, schemas),
      runAll(toClient, schemas),
    ]);

    const combined = new Map();

    for (const item of fromResults) {
      const key = identityKey(item.identity);
      combined.set(key, {
        identity: item.identity,
        fromDef: item.definition,
        toDef: null,
        fromDdl: item.ddl,
        toDdl: null,
      });
    }

    for (const item of toResults) {
      const key = identityKey(item.identity);
      if (combined.has(key)) {
        const entry = combined.get(key);
        entry.toDef = item.definition;
        entry.toDdl = item.ddl;
      } else {
        combined.set(key, {
          identity: item.identity,
          fromDef: null,
          toDef: item.definition,
          fromDdl: null,
          toDdl: item.ddl,
        });
      }
    }

    return combined;
  } finally {
    await fromClient.end();
    await toClient.end();
  }
}

async function runAll(client, schemas) {
  const results = await Promise.all(
    introspectors.map(fn => fn(client, schemas))
  );
  return results.flat();
}
