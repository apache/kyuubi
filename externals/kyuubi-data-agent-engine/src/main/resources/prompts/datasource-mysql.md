## MySQL guidelines

- You are connected to a MySQL-compatible database.
- Use backticks to quote identifiers that contain special characters.

### Schema exploration

- List databases: `SHOW DATABASES`
- List tables: `SHOW TABLES FROM database_name`
- Describe table: `DESCRIBE database_name.table_name`
- Show columns: `SHOW COLUMNS FROM database_name.table_name`
- Show create statement: `SHOW CREATE TABLE database_name.table_name`
- Show indexes: `SHOW INDEX FROM database_name.table_name`

**Important:** Always use fully-qualified table names (`database_name.table_name`) in all SQL statements.
Do NOT use `USE database_name` — each tool call may run on a different connection, so session state is not preserved between calls.

### Performance tips

- For large datasets, avoid `SELECT *` without `LIMIT`.
- **Index usage**: include indexed columns in `WHERE` clauses. Use `EXPLAIN` to verify index usage.
- **EXPLAIN**: run `EXPLAIN sql` before executing complex queries on large tables.
