# Drop and Recreate a Schema in a MotherDuck / DuckDB Database

Drops an existing schema (if it exists) in the specified database and
then creates a new empty schema. If the connection is to a MotherDuck
instance, the function switches to the given database first, then drops
and recreates the schema. Displays helpful CLI output about the current
connection, user, and database.

## Usage

``` r
delete_and_create_schema(.con, database_name, schema_name)
```

## Arguments

- .con:

  A valid `DBI` connection (DuckDB / MotherDuck).

- database_name:

  The name of the database where the schema should be dropped and
  recreated.

- schema_name:

  The name of the schema to drop and recreate.

## Value

Invisibly returns `NULL`. Side effect: drops and recreates the schema
and prints CLI status messages.

## Details

- Executes `DROP SCHEMA IF EXISTS ... CASCADE` to remove an existing
  schema and all contained objects.

- Executes `CREATE SCHEMA IF NOT EXISTS` to recreate it.

- If connected to MotherDuck (detected by
  [`validate_md_connection_status()`](validate_md_connection_status.md)),
  performs a `USE <database>` first.

- Prints a summary of the current connection and schema creation status
  using internal CLI helpers.

## See also

Other db-manage: [`alter_table_schema()`](alter_table_schema.md),
[`copy_tables_to_new_location()`](copy_tables_to_new_location.md),
[`create_database()`](create_database.md),
[`create_if_not_exists_share()`](create_if_not_exists_share.md),
[`create_or_replace_share()`](create_or_replace_share.md),
[`create_schema()`](create_schema.md),
[`create_table()`](create_table.md),
[`delete_database()`](delete_database.md),
[`delete_schema()`](delete_schema.md),
[`delete_table()`](delete_table.md),
[`describe_share()`](describe_share.md),
[`drop_share()`](drop_share.md),
[`list_owned_shares()`](list_owned_shares.md),
[`list_shared_with_me_shares()`](list_shared_with_me_shares.md),
[`upload_database_to_md()`](upload_database_to_md.md)
