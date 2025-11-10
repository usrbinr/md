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
[`copy_tables_to_new_location()`](https://usrbinr.github.io/md/reference/copy_tables_to_new_location.md),
[`create_database()`](https://usrbinr.github.io/md/reference/create_database.md),
[`create_if_not_exists_share()`](https://usrbinr.github.io/md/reference/create_if_not_exists_share.md),
[`create_or_replace_share()`](https://usrbinr.github.io/md/reference/create_or_replace_share.md),
[`create_schema()`](https://usrbinr.github.io/md/reference/create_schema.md),
[`create_table()`](https://usrbinr.github.io/md/reference/create_table.md),
[`delete_and_create_schema()`](https://usrbinr.github.io/md/reference/delete_and_create_schema.md),
[`delete_database()`](https://usrbinr.github.io/md/reference/delete_database.md),
[`delete_schema()`](https://usrbinr.github.io/md/reference/delete_schema.md),
[`delete_table()`](https://usrbinr.github.io/md/reference/delete_table.md),
[`describe_share()`](https://usrbinr.github.io/md/reference/describe_share.md),
[`drop_share()`](https://usrbinr.github.io/md/reference/drop_share.md),
[`list_owned_shares()`](https://usrbinr.github.io/md/reference/list_owned_shares.md),
[`list_shared_with_me_shares()`](https://usrbinr.github.io/md/reference/list_shared_with_me_shares.md),
[`upload_database_to_md()`](https://usrbinr.github.io/md/reference/upload_database_to_md.md)
