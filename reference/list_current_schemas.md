# List Schemas in the Current Database

Returns a lazy tibble of all schemas in the **current database** of the
connection. Queries `information_schema.schemata` and filters to the
current database (`catalog_name = current_database()`).

## Usage

``` r
list_current_schemas(.con)
```

## Arguments

- .con:

  A valid `DBI` connection (DuckDB / MotherDuck).

## Value

A `dbplyr` lazy tibble with columns:

- `catalog_name` — the current database name.

- `schema_name` — each schema within that database.

## Details

- This function assumes the connection is valid (checked with
  [`validate_con()`](validate_con.md)).

- Returns a `dbplyr` lazy table; use `collect()` to bring the result
  into R.

## See also

Other db-list: [`list_all_databases()`](list_all_databases.md),
[`list_all_tables()`](list_all_tables.md),
[`list_current_tables()`](list_current_tables.md),
[`list_extensions()`](list_extensions.md), [`list_fns()`](list_fns.md),
[`list_setting()`](list_setting.md), [`list_shares()`](list_shares.md)
