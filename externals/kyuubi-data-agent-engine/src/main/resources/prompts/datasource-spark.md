## Spark SQL guidelines

- You are connected to a Spark SQL engine via Kyuubi.
- Use backticks to quote identifiers that contain special characters.
- Spark supports ANSI SQL, HiveQL extensions, and Delta Lake syntax.

### Schema exploration

- List databases: `SHOW DATABASES`
- List tables: `SHOW TABLES IN database_name`
- Describe table: `DESCRIBE TABLE database_name.table_name` or `DESCRIBE TABLE EXTENDED database_name.table_name`
- Show columns: `SHOW COLUMNS IN database_name.table_name`
- Show create statement: `SHOW CREATE TABLE database_name.table_name`
- Show partitions: `SHOW PARTITIONS database_name.table_name`

**Important:** Always use fully-qualified table names (`database_name.table_name`) in all SQL statements.
Do NOT use `USE database_name` — each tool call may run on a different connection, so session state like the current database is not preserved between calls.

### Estimating table size

- Use `DESCRIBE TABLE EXTENDED table_name` and check the `Statistics` row (e.g. `7783057 bytes`) to estimate size without scanning data.
- For partitioned tables, `SHOW PARTITIONS` reveals partition count — a rough proxy for data volume.
- If metadata is unavailable, use `SELECT COUNT(*) FROM table TABLESAMPLE(1 PERCENT) * 100` for a fast estimate.

### Performance tips

- Prefer built-in functions: `explode`, `collect_list`, window functions, etc.
- For large datasets, avoid `SELECT *` without `LIMIT`.
- **Partition pruning**: always include the partition column (often `*_date_sk`, `dt`, `ds`) in `WHERE` clauses. Check `EXPLAIN` output for `PartitionFilters` to verify pruning is effective.
- **Broadcast joins**: Spark auto-broadcasts small tables in JOINs. When joining a large fact table with dimension tables, put dimension filters in the `WHERE` clause so the optimizer can apply dynamic partition pruning.
- **Sampling**: use `TABLESAMPLE(N PERCENT)` for approximate statistics on large tables. Multiply results by `100/N` to estimate full-table values.
- **Approximate functions**: use `APPROX_COUNT_DISTINCT(col)` instead of `COUNT(DISTINCT col)` for cardinality estimation on large tables — it is orders of magnitude faster.
- **EXPLAIN**: run `EXPLAIN sql` before executing complex queries on large tables. Check for:
  - `BroadcastHashJoin` (good) vs `SortMergeJoin` (expensive shuffle)
  - `PartitionFilters` and `PushedFilters` (predicate pushdown working)
  - `dynamicpruningexpression` (dynamic partition pruning active)