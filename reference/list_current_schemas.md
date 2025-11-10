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
  [`validate_con()`](https://usrbinr.github.io/md/reference/validate_con.md)).

- Returns a `dbplyr` lazy table; use
  [`collect()`](https://dplyr.tidyverse.org/reference/compute.html) to
  bring the result into R.

## See also

Other db-list:
[`list_all_databases()`](https://usrbinr.github.io/md/reference/list_all_databases.md),
[`list_all_tables()`](https://usrbinr.github.io/md/reference/list_all_tables.md),
[`list_current_tables()`](https://usrbinr.github.io/md/reference/list_current_tables.md),
[`list_extensions()`](https://usrbinr.github.io/md/reference/list_extensions.md),
[`list_fns()`](https://usrbinr.github.io/md/reference/list_fns.md),
[`list_setting()`](https://usrbinr.github.io/md/reference/list_setting.md),
[`list_shares()`](https://usrbinr.github.io/md/reference/list_shares.md)
