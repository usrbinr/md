# Change Active Database and Schema

Switches the active database and (optionally) schema for a valid DuckDB
or MotherDuck connection. The function validates the target database and
schema before executing the `USE` command and provides user feedback via
CLI messages.

## Usage

``` r
cd(.con, database_name, schema_name)
```

## Arguments

- .con:

  A valid `DBI` connection (DuckDB / MotherDuck).

- database_name:

  A character string specifying the database to switch to. Must be one
  of the available databases returned by
  [`list_all_databases()`](https://usrbinr.github.io/md/reference/list_all_databases.md).

- schema_name:

  (Optional) A character string specifying the schema to switch to
  within the given database. Must be one of the available schemas
  returned by
  [`list_current_schemas()`](https://usrbinr.github.io/md/reference/list_current_schemas.md).

## Value

Invisibly returns a message summarizing the new connection context. Side
effects include printing CLI headers showing the current user and
database context.

## Details

The `cd()` function is analogous to a "change directory" command in a
file system, but for database contexts. It updates the currently active
database (and optionally schema) for the given connection. If the target
database or schema does not exist, the function aborts with a
descriptive CLI error message.

## See also

Other db-meta:
[`launch_ui()`](https://usrbinr.github.io/md/reference/launch_ui.md),
[`pwd()`](https://usrbinr.github.io/md/reference/pwd.md)

## Examples

``` r
if (FALSE) { # \dontrun{
# Connect to MotherDuck
con <- DBI::dbConnect(duckdb::duckdb(), dbdir = "md:")

# List available databases
list_databases(con)

# Change to a specific database and schema
cd(con, database_name = "analytics_db", schema_name = "public")

# Disconnect
DBI::dbDisconnect(con)
} # }
```
