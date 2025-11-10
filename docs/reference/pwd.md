# Print Current MotherDuck Database Context

Displays the current database, schema, and role for the active
DuckDB/MotherDuck connection. This mirrors the behavior of `pwd` in
Linux by showing your current “working database.”

## Usage

``` r
pwd(.con)
```

## Arguments

- .con:

  A valid `DBI` connection (DuckDB / MotherDuck).

## Value

A tibble with columns:

- current_database:

  The active database name.

- current_schema:

  The active schema name.

The current role is printed to the console via `cli`.

## Details

The `pwd()` function is a helper for inspecting the current database
context of a DuckDB or MotherDuck connection. It queries the database
for the current database, schema, and role. The database and schema are
returned as a tibble for easy programmatic access, while the role is
displayed using a CLI alert. This is especially useful in multi-database
environments or when working with different user roles, providing a
quick way to verify where SQL queries will be executed.

## See also

Other db-meta: [`cd()`](cd.md), [`launch_ui()`](launch_ui.md)

## Examples

``` r
if (FALSE) { # \dontrun{
con <- DBI::dbConnect(duckdb::duckdb())
pwd(con)
} # }
```
