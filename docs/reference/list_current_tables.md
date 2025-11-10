# List Tables in the Current Database and Schema

Returns a lazy tibble of all tables that exist in the **current
database** and **current schema** of the active connection. Queries the
standard `information_schema.tables` view and filters to
`current_database()` and `current_schema()`.

## Usage

``` r
list_current_tables(.con)
```

## Arguments

- .con:

  A valid `DBI` connection (DuckDB / MotherDuck).

## Value

A `dbplyr` lazy tibble with columns:

- `table_catalog` — the current database.

- `table_schema` — the current schema.

- `table_name` — each table name.

## Details

- This function validates that the connection is valid with
  [`validate_con()`](validate_con.md).

- Result is a `dbplyr` lazy table (`tbl_dbi`); call `collect()` to bring
  it into R.

## See also

Other db-list: [`list_all_databases()`](list_all_databases.md),
[`list_all_tables()`](list_all_tables.md),
[`list_current_schemas()`](list_current_schemas.md),
[`list_extensions()`](list_extensions.md), [`list_fns()`](list_fns.md),
[`list_setting()`](list_setting.md), [`list_shares()`](list_shares.md)
