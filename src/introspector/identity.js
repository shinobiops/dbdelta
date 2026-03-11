export function identity(schema, type, name) {
  return { schema, type, name };
}

export function identityKey(id) {
  return `${id.schema}.${id.type}.${id.name}`;
}
