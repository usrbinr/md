# List All Tables Visible to the Connection

Returns a lazy tibble of all tables visible to the current connection by
querying `information_schema.tables` (across all catalogs/databases and
schemas).

## Usage

``` r
list_all_tables(.con)
```

## Arguments

- .con:

  A valid `DBI` connection (DuckDB / MotherDuck).

## Value

A `dbplyr` lazy tibble with columns:

- `table_catalog` — database/catalog name

- `table_schema` — schema name

- `table_name` — table name

## Details

The result is a `dbplyr` lazy table (`tbl_dbi`). Use `collect()` to
bring results into R as a local tibble.

## See also

Other db-list: [`list_all_databases()`](list_all_databases.md),
[`list_current_schemas()`](list_current_schemas.md),
[`list_current_tables()`](list_current_tables.md),
[`list_extensions()`](list_extensions.md), [`list_fns()`](list_fns.md),
[`list_setting()`](list_setting.md), [`list_shares()`](list_shares.md)
