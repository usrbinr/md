# Upload a Local Database to MotherDuck

Creates a new database on MotherDuck (if it does not exist) and copies
all objects from an existing local database into it using the
`COPY FROM DATABASE` command.

## Usage

``` r
upload_database_to_md(.con, from_db_name, to_db_name)
```

## Arguments

- .con:

  A valid `DBI` connection (DuckDB / MotherDuck).

- from_db_name:

  The local database name to copy from.

- to_db_name:

  The target MotherDuck database to create/overwrite.

## Value

Invisibly returns `NULL`. Side effect: creates the target database and
copies all objects; prints a CLI action report.

## Details

- Runs `CREATE DATABASE <to_db_name>` if the target database does not
  exist.

- Then runs `COPY FROM DATABASE <from_db_name> TO <to_db_name>` to copy
  all objects (tables, views, etc.) from the local database.

- Prints a CLI status report (connection, user, current DB) after
  completion.

## See also

Other db-manage: [`alter_table_schema()`](alter_table_schema.md),
[`copy_tables_to_new_location()`](copy_tables_to_new_location.md),
[`create_database()`](create_database.md),
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
[`list_shared_with_me_shares()`](list_shared_with_me_shares.md)

## Examples

``` r
if (FALSE) { # \dontrun{
con_db <- DBI::dbConnect(duckdb::duckdb())
create_table(.con=con_db,.data=mtcars,database_name="memory",schema_name="main",table_name="mtcars")
con_md <- connect_to_motherduck()

upload_database_to_md(con_md, from_db_name = "memory", to_db_name = "analytics")
} # }
```
