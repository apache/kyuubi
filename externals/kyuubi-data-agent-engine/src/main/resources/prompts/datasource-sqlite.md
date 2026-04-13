## SQLite SQL compatibility

SQLite differs significantly from MySQL/PostgreSQL. Follow these rules strictly.

### Schema exploration

- List tables: `SELECT name FROM sqlite_master WHERE type='table' ORDER BY name`
- Describe table: `PRAGMA table_info(table_name)`
- Show indexes: `PRAGMA index_list(table_name)`
- Sample data: `SELECT * FROM table_name LIMIT 3`

### Type casting
- `CAST(x AS REAL)` — no FLOAT/DOUBLE. `CAST(x AS INTEGER)` — no INT/BIGINT.
- `CAST(x AS TEXT)` — no VARCHAR/CHAR(n). No BOOLEAN type — use 1/0.

### Date and time
- No `NOW()` or `CURDATE()`. Use `DATE('now')`, `DATETIME('now')`.
- Arithmetic: `DATE(col, '+7 days')`, `DATE(col, 'start of year')` — no DATEADD/INTERVAL.
- Extract: `STRFTIME('%Y', col)` for year — no YEAR(), EXTRACT(), MONTH().
- Difference: `JULIANDAY(d1) - JULIANDAY(d2)` — no DATEDIFF().

### String functions
- Concatenation: `||` — no CONCAT(). Substring: `SUBSTR()` — no SUBSTRING()/LEFT()/RIGHT().
- Locate: `INSTR(haystack, needle)` — no LOCATE()/CHARINDEX()/POSITION().
- `GROUP_CONCAT(x, ',')` — not `GROUP_CONCAT(x SEPARATOR ',')`.
- No LPAD/RPAD.

### Math functions
- No CEIL/FLOOR/LOG/LN/EXP/POWER/SQRT. Modulo: `x % y`.
- `ROUND(x, n)`, `ABS(x)`, `MAX(a, b)`, `MIN(a, b)` are available.

### Unsupported features
- No GREATEST/LEAST — use scalar MAX()/MIN().
- No IF() — use IIF(cond, t, f) or CASE WHEN.
- No regex — use LIKE with %/_ or GLOB with */?
- Booleans: use 1/0 — no TRUE/FALSE literals.
- Quote identifiers with double quotes `"col"` — not brackets [col].