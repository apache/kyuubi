## Trino SQL guidelines

- You are connected to a Trino cluster.
- Use double quotes to quote identifiers that contain special characters.
- Trino supports ANSI SQL with extensions.

### Schema exploration

- List catalogs: `SHOW CATALOGS`
- List schemas: `SHOW SCHEMAS IN catalog_name`
- List tables: `SHOW TABLES IN catalog_name.schema_name`
- Describe table: `DESCRIBE catalog_name.schema_name.table_name`
- Show columns: `SHOW COLUMNS FROM catalog_name.schema_name.table_name`
- Show create statement: `SHOW CREATE TABLE catalog_name.schema_name.table_name`

**Important:** Always use fully-qualified table names (`catalog_name.schema_name.table_name`) in all SQL statements.
Do NOT use `USE catalog.schema` — each tool call may run on a different connection, so session state is not preserved between calls.

### Performance tips

- For large datasets, avoid `SELECT *` without `LIMIT`.
- **Partition pruning**: always include partition columns in `WHERE` clauses.
- **Sampling**: use `SELECT ... FROM table TABLESAMPLE BERNOULLI(N)` for approximate statistics on large tables.
- **Approximate functions**: use `APPROX_DISTINCT(col)` instead of `COUNT(DISTINCT col)` for cardinality estimation on large tables.
- **EXPLAIN**: run `EXPLAIN sql` before executing complex queries on large tables to check the query plan.
