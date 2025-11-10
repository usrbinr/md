# Create (If Not Exists) and Switch to a Database

Ensures a database exists and sets it as the active database. If
connected to MotherDuck, the function will run
`CREATE DATABASE IF NOT EXISTS` followed by `USE <database>`. Prints CLI
status information about the current user and database.

## Usage

``` r
create_database(.con, database_name)
```

## Arguments

- .con:

  A valid `DBI` connection (DuckDB / MotherDuck).

- database_name:

  Name of the database to create/ensure and switch to

## Value

Invisibly returns `NULL`. Side effect: may create a database and
switches to it; prints CLI status

## Details

- Connection type is checked via
  [`validate_md_connection_status()`](validate_md_connection_status.md)
  (with `return_type = "arg"`).

- On MotherDuck, executes:

  - `CREATE DATABASE IF NOT EXISTS <database>`

  - `USE <database>`

- Displays status and environment info with CLI messages.

## See also

Other db-manage: [`alter_table_schema()`](alter_table_schema.md),
[`copy_tables_to_new_location()`](copy_tables_to_new_location.md),
[`create_if_not_exists_share()`](create_if_not_exists_share.md),
[`create_or_replace_share()`](create_or_replace_share.md),
[`create_schema()`](create_schema.md),
[`create_table()`](create_table.md),
[`delete_and_create_schema()`](delete_and_create_schema.md),
[`delete_database()`](delete_database.md),
[`delete_schema()`](delete_schema.md),
[`delete_table()`](delete_table.md),
[`describe_share()`](describe_share.md),
[`drop_share()`](drop_share.md),
[`list_owned_shares()`](list_owned_shares.md),
[`list_shared_with_me_shares()`](list_shared_with_me_shares.md),
[`upload_database_to_md()`](upload_database_to_md.md)

## Examples

``` r
if (FALSE) { # \dontrun{
con_md <- connect_to_motherduck()
create_database(con_md, "analytics")
} # }
```
