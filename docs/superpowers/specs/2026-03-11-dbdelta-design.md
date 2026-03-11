# dbdelta — PostgreSQL Database Migration Diff Tool

## Overview

A Node.js CLI tool that compares two PostgreSQL databases and generates a migration SQL script to transform the first database's schema into the second's. Outputs to stdout.

## CLI Interface

```
npx dbdelta <fromUrl> <toUrl> [options]

Options:
  --schemas <list>          Comma-separated schemas to compare (default: public)
  --rename <spec>           Rename mapping, repeatable
                            e.g. --rename table:old:new
                            e.g. --rename column:schema.table/old_col:new_col
  --exclude-schemas <list>  Schemas to skip entirely
  --exclude-types <list>    Object types to skip (e.g. "grants,roles")
```

- Output: SQL to stdout
- Progress/diagnostics: stderr
- No transaction wrapping (output includes a warning comment about partial failure risk)
- Destructive operations (drops): included but commented out
- No connection string info in output

### Rename Syntax

The `--rename` flag uses `/` to separate the table from the column (avoiding ambiguity with schema-qualified names):

- `--rename table:old_name:new_name` — rename a table
- `--rename column:schema.table/old_col:new_col` — rename a column (schema-qualified)
- `--rename column:table/old_col:new_col` — rename a column (defaults to `public` schema)

## Architecture

```
CLI (args parsing)
  -> Introspector (queries both DBs in parallel)
    -> DependencyGraph (from pg_depend, topological sort)
      -> Differ (compares matched objects, produces change operations)
        -> SQLGenerator (emits ordered SQL to stdout)
```

Single dependency: `pg` (node-postgres). Plain Node.js with ESM modules, no build step.

## Package Structure

```
dbdelta/
  package.json
  src/
    cli.js                # arg parsing, entry point
    introspector/
      index.js            # parallel introspection orchestrator
      schemas.js
      extensions.js
      types.js
      tables.js
      columns.js
      constraints.js
      indexes.js
      sequences.js
      functions.js
      views.js
      triggers.js
      policies.js
      operators.js
      aggregates.js
      casts.js
      collations.js
      text-search.js
      fdw.js
      publications.js
      subscriptions.js
      roles.js
      grants.js
      rules.js
      event-triggers.js
      statistics.js
      user-mappings.js
      opclasses.js
    dependencies.js       # pg_depend graph + topological sort
    differ.js             # matching + diffing logic
    sql-generator.js      # SQL emission per change type
```

## Object Types

### Schema-level
- Schemas (`pg_namespace`)
- Extensions (`pg_extension`)
- Types — enums, composites, domains, ranges (`pg_type`, `pg_enum`, `pg_range`)
- Sequences (`pg_sequence`, `pg_class`)
- Collations (`pg_collation`)
- Text search configs and dictionaries (`pg_ts_config`, `pg_ts_dict`)
- Text search parsers and templates — only diffed when created by extensions; output includes a comment noting these require C functions (`pg_ts_parser`, `pg_ts_template`)
- Statistics objects (`pg_statistic_ext`)

### Table-level
- Tables + columns + defaults + storage params (`pg_class`, `pg_attribute`, `pg_attrdef`)
- Constraints — PK, FK, unique, check, exclusion (`pg_constraint`)
- Indexes (`pg_index`, `pg_class`)
- Triggers (`pg_trigger`)
- RLS policies (`pg_policy`)
- Partitioning info (`pg_partitioned_table`, `pg_inherits`)

### Code objects
- Functions & procedures (`pg_proc`)
- Views (`pg_views` / `pg_class` + `pg_rewrite`)
- Materialized views (`pg_matviews` / `pg_class`) — generated DDL includes `WITH NO DATA` by default
- Rules (`pg_rewrite`)

### Access control
- Roles (`pg_roles`)
- Grants/privileges (from `relacl`, `proacl`, `nspacl`, etc.)
- Default privileges (`pg_default_acl`)

### Other
- Operators (`pg_operator`)
- Operator classes & families (`pg_opclass`, `pg_opfamily`)
- Aggregates (`pg_aggregate`)
- Casts (`pg_cast`)
- Foreign data wrappers, servers, tables, user mappings (`pg_foreign_data_wrapper`, `pg_foreign_server`, `pg_foreign_table`, `pg_user_mapping`)
- Event triggers (`pg_event_trigger`)
- Publications (`pg_publication`)
- Subscriptions (`pg_subscription`)

## Diffing & Change Operations

### Matching Logic
- Objects matched by `schema + type + name`
- `--rename` overrides matching (e.g., `--rename table:users:accounts`)
- Column-level renames: `--rename column:schema.table/old_col:new_col`
- Unmatched in `fromUrl` -> DROP (commented out)
- Unmatched in `toUrl` -> CREATE
- Matched with different definitions -> ALTER or CREATE OR REPLACE

### Change Types Per Object

| Object Type | Can ALTER? | Can CREATE OR REPLACE? | Must DROP+CREATE? |
|---|---|---|---|
| Schema | yes (rename, owner) | no | no |
| Table | yes (add/drop/alter columns, constraints) | no | no |
| Column | yes (type, default, not null) | no | no |
| Index | no | no | yes |
| Constraint | no | no | yes |
| Function/Procedure | yes (owner, volatility, security, cost, SET) | yes (body, args, return type) | no |
| View | no | yes | no |
| Materialized View | no | no | yes |
| Trigger | no | no | yes |
| Type (enum) | yes (add value) | no | yes (if removing values) |
| Type (composite) | yes (add/alter attribute) | no | yes (if dropping attribute with dependents) |
| Domain | yes (constraints, default) | no | no |
| Sequence | yes (owned by, params) | no | no |
| Extension | yes (version) | no | no |
| RLS Policy | yes (using, with check, roles) | no | no |
| Operator | no | no | yes |
| Aggregate | no | no | yes |
| Cast | no | no | yes |
| Statistics | no | no | yes |
| FDW/Server/Foreign Table | yes (options) | no | no |
| User Mapping | yes (options) | no | no |
| Publication/Subscription | yes | no | no |
| Grants | GRANT/REVOKE | no | no |

When DROP+CREATE is required, the dependency graph informs what else must be dropped and recreated. Comments explain side effects.

## Dependency Graph & Ordering

### Building the Graph
- Query `pg_depend` from both `toUrl` and `fromUrl`
- `toUrl` graph: used for ordering creates and alters
- `fromUrl` graph: used for ordering drops and detecting cascade impacts
- Directed graph: edge from A -> B means "A depends on B"

### Cascade Detection
When an object must be dropped (either for removal or for drop+recreate), the `fromUrl` dependency graph identifies all dependents. If a dependent is being kept (exists in both databases), it must be temporarily dropped and recreated. The tool emits this full sequence with comments explaining why each intermediate drop/recreate is needed.

### Ordering Output

All operations are interleaved into a single dependency-ordered sequence rather than separated into rigid sections. The topological sort produces a unified order where:

- Dependencies are created before their dependents
- Dependents are dropped before their dependencies
- Alters are placed at the correct point relative to creates/drops

The output is grouped by phase for readability, but the ordering within and across phases respects the full dependency graph:

```sql
-- dbdelta migration
-- Generated: <timestamp>
-- WARNING: no transaction wrapper; failures may leave the database in a partial state

-- === PHASE 1: DROPS (commented out -- review carefully) ===
-- <dependency-ordered drops>

-- === PHASE 2: CREATES & ALTERS ===
-- <dependency-ordered creates and alters, interleaved as needed>

-- === PHASE 3: GRANTS/REVOKES ===
-- <alphabetical>
```

## Introspection Strategy

Each introspector module:
1. Runs a catalog query against both databases in parallel
2. Returns an array of normalized objects with:
   - `identity`: `{schema, type, name}` — used for matching
   - `definition`: full object definition — used for comparison
   - `ddl`: function to generate CREATE/ALTER/DROP SQL

Comparison is done on the `definition` object (deep equality). When objects differ, the differ determines which change strategy applies (ALTER, CREATE OR REPLACE, or DROP+CREATE) based on the object type and what specifically changed.

## Configuration

- `--schemas`: comma-separated list (default: `public`)
- `--exclude-schemas`: comma-separated list of schemas to skip
- `--exclude-types`: comma-separated list of object types to skip
- `--rename`: repeatable flag for rename mappings
- System schemas always excluded: `pg_catalog`, `information_schema`, `pg_toast`
