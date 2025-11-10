# Create a Schema in a Database if It Does Not Exist

Ensures that a specified schema exists in the given database. If the
connection is to a MotherDuck instance, the function switches to the
specified database before creating the schema. It also prints helpful
connection and environment information via CLI messages for
transparency.

## Usage

``` r
create_schema(.con, database_name, schema_name)
```

## Arguments

- .con:

  A valid `DBI` connection (DuckDB / MotherDuck).

- database_name:

  Name of the database to create/use.

- schema_name:

  Name of the schema to create if it does not exist.

## Value

Invisibly returns `NULL`. Side effect: creates the schema if necessary
and prints CLI messages.

## Details

- Uses
  [`DBI::dbExecute()`](https://dbi.r-dbi.org/reference/dbExecute.html)
  with `CREATE SCHEMA IF NOT EXISTS` to create the schema only when
  needed.

- If connected to MotherDuck (determined by
  [`validate_md_connection_status()`](validate_md_connection_status.md)),
  executes `USE <database>` before creating the schema.

- Displays connection/user/database information via internal CLI
  helpers.

## See also

Other db-manage: [`alter_table_schema()`](alter_table_schema.md),
[`copy_tables_to_new_location()`](copy_tables_to_new_location.md),
[`create_database()`](create_database.md),
[`create_if_not_exists_share()`](create_if_not_exists_share.md),
[`create_or_replace_share()`](create_or_replace_share.md),
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
