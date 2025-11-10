# Move Tables from One Schema to Another

Moves one or more tables from an existing schema to a new (target)
schema using `ALTER TABLE ... SET SCHEMA`. If the target schema does not
exist, it is created first.

## Usage

``` r
alter_table_schema(.con, from_table_names, new_schema)
```

## Arguments

- .con:

  A valid `DBI` connection (DuckDB / MotherDuck).

- from_table_names:

  Character vector of table names to move.

- new_schema:

  Target schema name (where the tables will be moved).

## Value

Invisibly returns a character vector of fully-qualified table names
moved. Side effects: creates `new_schema` if needed and alters table
schemas.

## Details

- Ensures `new_schema` exists (`CREATE SCHEMA IF NOT EXISTS`).

- For each table in `table_names`, runs:
  `ALTER TABLE old_schema.table SET SCHEMA new_schema`.

- Table and schema identifiers are safely quoted with
  [`glue::glue_sql()`](https://glue.tidyverse.org/reference/glue_sql.html).

## See also

Other db-manage:
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
[`list_shared_with_me_shares()`](list_shared_with_me_shares.md),
[`upload_database_to_md()`](upload_database_to_md.md)
