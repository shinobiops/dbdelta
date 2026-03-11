import { connect } from '../connection.js';
import { identityKey } from './identity.js';
import { introspectSchemas } from './schemas.js';
import { introspectExtensions } from './extensions.js';

const introspectors = [
  introspectSchemas,
  introspectExtensions,
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
