# List Database Settings

The `list_setting()` function provides a convenient way to inspect the
active configuration of a DuckDB or MotherDuck connection. It executes
the internal DuckDB function `duckdb_settings()` and returns the results
as a tidy tibble for easy viewing or filtering.

## Usage

``` r
list_setting(.con)
```

## Arguments

- .con:

  A valid `DBI` connection (DuckDB / MotherDuck).

## Value

A [tibble::tibble](https://tibble.tidyverse.org/reference/tibble.html)
containing one row per setting with columns describing the setting name,
current value, description, and default value.

## Details

This function is particularly useful for debugging or auditing runtime
environments. All settings are returned as character columns, including
their names, current values, and default values.

## See also

Other db-list: [`list_all_databases()`](list_all_databases.md),
[`list_all_tables()`](list_all_tables.md),
[`list_current_schemas()`](list_current_schemas.md),
[`list_current_tables()`](list_current_tables.md),
[`list_extensions()`](list_extensions.md), [`list_fns()`](list_fns.md),
[`list_shares()`](list_shares.md)

## Examples

``` r
if (FALSE) { # \dontrun{
# Connect to DuckDB
con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")

# List all database settings
list_setting(con)

# Disconnect
DBI::dbDisconnect(con)
} # }
```
