# Drop a Database

Drops a database from the current DuckDB or MotherDuck connection if it
exists. Prints a CLI status report after performing the operation.

## Usage

``` r
delete_database(.con, database_name)
```

## Arguments

- .con:

  A valid `DBI` connection (DuckDB / MotherDuck).

- database_name:

  Name of the database to drop.

## Value

Invisibly returns `NULL`. Side effect: drops the database and prints CLI
status messages.

## Details

- Executes `DROP DATABASE IF EXISTS <database_name>` to remove the
  database.

- Intended for DuckDB or MotherDuck connections.

- Prints user, database and action details using CLI helper functions.

## See also

Other db-manage: [`alter_table_schema()`](alter_table_schema.md),
[`copy_tables_to_new_location()`](copy_tables_to_new_location.md),
[`create_database()`](create_database.md),
[`create_if_not_exists_share()`](create_if_not_exists_share.md),
[`create_or_replace_share()`](create_or_replace_share.md),
[`create_schema()`](create_schema.md),
[`create_table()`](create_table.md),
[`delete_and_create_schema()`](delete_and_create_schema.md),
[`delete_schema()`](delete_schema.md),
[`delete_table()`](delete_table.md),
[`describe_share()`](describe_share.md),
[`drop_share()`](drop_share.md),
[`list_owned_shares()`](list_owned_shares.md),
[`list_shared_with_me_shares()`](list_shared_with_me_shares.md),
[`upload_database_to_md()`](upload_database_to_md.md)
