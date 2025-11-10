# List Database Functions (DuckDB/MotherDuck)

Returns a lazy table listing available SQL functions from the current
DuckDB/MotherDuck connection using `duckdb_functions()`.

## Usage

``` r
list_fns(.con)
```

## Arguments

- .con:

  A valid `DBI` connection (DuckDB / MotherDuck).

## Value

A `dbplyr` lazy tibble (`tbl_dbi`) with function metadata (e.g.,
`function_name`, `schema`, `is_aggregate`, `is_alias`, etc.).

## Details

This wrapper validates the connection and then queries
`duckdb_functions()` to enumerate function metadata. The result is a
`dbplyr` lazy tibble (`tbl_dbi`); call `collect()` to materialize it in
R.

## See also

Other db-list: [`list_all_databases()`](list_all_databases.md),
[`list_all_tables()`](list_all_tables.md),
[`list_current_schemas()`](list_current_schemas.md),
[`list_current_tables()`](list_current_tables.md),
[`list_extensions()`](list_extensions.md),
[`list_setting()`](list_setting.md), [`list_shares()`](list_shares.md)
