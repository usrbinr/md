# Copy Tables to a New Database/Schema

Copies one or more tables to a new location (database/schema) by
creating new tables via `CREATE TABLE ... AS SELECT * FROM ...`.
Requires motherduck connection

## Usage

``` r
copy_tables_to_new_location(
  .con,
  from_table_names,
  to_database_name,
  to_schema_name
)
```

## Arguments

- .con:

  A valid `DBI` connection (DuckDB / MotherDuck).

- from_table_names:

  A tibble/data frame listing source tables, with columns
  `database_name`, `schema_name`, and `table_name`.

- to_database_name:

  Target database name.

- to_schema_name:

  Target schema name.

## Value

Invisibly returns a character vector of fully-qualified destination
table names that were created. Side effect: creates target DB/schema if
needed and writes new tables.

## Details

- Input `from_table_names` must contain columns: `database_name`,
  `schema_name`, and `table_name`.

- For each source table, the function issues:
  `CREATE TABLE <to_db>.<to_schema>.<table> AS SELECT * FROM <src_db>.<src_schema>.<table>`.

- On local DuckDB (non-MotherDuck), the target database name is ignored
  and defaults to the current database of the connection.

## See also

Other db-manage: [`alter_table_schema()`](alter_table_schema.md),
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
[`list_shared_with_me_shares()`](list_shared_with_me_shares.md),
[`upload_database_to_md()`](upload_database_to_md.md)
