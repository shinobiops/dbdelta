# dbdelta

Compare two PostgreSQL databases and generate migration SQL to transform one schema into another.

dbdelta introspects both databases in parallel, builds a dependency graph, diffs every object, and emits a dependency-ordered migration script.

## Install

```bash
npm install -g dbdelta
```

## Usage

```bash
dbdelta <source-connection> <target-connection> [options]
```

SQL is written to stdout, progress is logged to stderr, so you can pipe directly to a file:

```bash
dbdelta postgres://localhost/current postgres://localhost/desired > migration.sql
```

### Options

| Flag | Description |
|------|-------------|
| `--schemas` | Comma-separated list of schemas to compare (default: `public`) |
| `--exclude-schemas` | Schemas to skip |
| `--exclude-types` | Object types to skip (e.g. `grants,roles`) |
| `--rename` | Map renamed objects: `type:old_name:new_name` |
| `--help` | Show help |

### Rename example

If you renamed a table, tell dbdelta so it diffs instead of dropping and recreating:

```bash
dbdelta src dst --rename table:old_users:users
```

## Supported objects

dbdelta covers 28 PostgreSQL object types:

- **Schema-level** — schemas, extensions, sequences, collations, statistics
- **Types** — enums, composites, domains, ranges
- **Tables** — tables, columns, constraints, indexes
- **Code** — functions, procedures, views, materialized views, rules
- **Triggers & policies** — triggers, RLS policies, event triggers
- **Operators** — operators, operator classes, aggregates, casts
- **Text search** — configs, dictionaries, parsers, templates
- **Foreign data** — FDWs, foreign servers, foreign tables, user mappings
- **Replication** — publications, subscriptions
- **Access control** — roles, grants, default privileges

## How it works

```
CLI (parse args)
  → Introspect both databases in parallel
    → Build dependency graph from pg_depend
      → Diff objects by schema + type + name
        → Emit dependency-ordered SQL
```

The output has three phases:

1. **Drops** — commented out for manual review
2. **Creates & alters** — ordered by dependency graph
3. **Grants** — alphabetical

DROP statements are always commented out. You review and uncomment what you need.

No `BEGIN`/`COMMIT` wrapping — you control the transaction boundary.

## License

[MIT](LICENSE)
