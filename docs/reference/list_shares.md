# List MotherDuck Shares

The `list_shares()` function provides a convenient wrapper around the
MotherDuck SQL command `LIST SHARES;`. It validates that the supplied
connection is an active MotherDuck connection before executing the
query. If the connection is not valid, the function returns `0` instead
of a table.

## Usage

``` r
list_shares(.con)
```

## Arguments

- .con:

  A valid `DBI` connection (DuckDB / MotherDuck).

## Value

A [tibble::tibble](https://tibble.tidyverse.org/reference/tibble.html)
containing details of available shares if the connection is an MD
connection or an empty tibble if not

## Details

MotherDuck supports object sharing, which allows users to list and
access data shared between accounts. This function helps
programmatically inspect available shares within an authenticated
MotherDuck session.

## See also

Other db-list: [`list_all_databases()`](list_all_databases.md),
[`list_all_tables()`](list_all_tables.md),
[`list_current_schemas()`](list_current_schemas.md),
[`list_current_tables()`](list_current_tables.md),
[`list_extensions()`](list_extensions.md), [`list_fns()`](list_fns.md),
[`list_setting()`](list_setting.md)

## Examples

``` r
if (FALSE) { # \dontrun{
# Connect to MotherDuck
con <- DBI::dbConnect(duckdb::duckdb(), dbdir = "md:")

# List shares
list_shares(con)

# Disconnect
DBI::dbDisconnect(con)
} # }
```
