# Drop a Schema from a Database

Drops a schema from a specified database. Optionally cascades the
deletion to all objects within the schema. Prints helpful CLI
information about the current connection and action.

## Usage

``` r
delete_schema(.con, database_name, schema_name, cascade = FALSE)
```

## Arguments

- .con:

  A valid `DBI` connection (DuckDB / MotherDuck).

- database_name:

  Name of the database containing the schema.

- schema_name:

  Name of the schema to drop.

- cascade:

  Logical; if `TRUE` (default), use `CASCADE` to drop all dependent
  objects in the schema. If `FALSE`, drop only if empty.

## Value

Invisibly returns `NULL`. Side effect: drops the schema (and contained
objects if `cascade = TRUE`) and prints CLI status.

## Details

- Runs `DROP SCHEMA IF EXISTS <db>.<schema>` with optional `CASCADE`.

- Intended for DuckDB or MotherDuck connections.

- Uses CLI helpers to show current connection and report the deletion.

## See also

Other db-manage:
[`alter_table_schema()`](https://usrbinr.github.io/md/reference/alter_table_schema.md),
[`copy_tables_to_new_location()`](https://usrbinr.github.io/md/reference/copy_tables_to_new_location.md),
[`create_database()`](https://usrbinr.github.io/md/reference/create_database.md),
[`create_if_not_exists_share()`](https://usrbinr.github.io/md/reference/create_if_not_exists_share.md),
[`create_or_replace_share()`](https://usrbinr.github.io/md/reference/create_or_replace_share.md),
[`create_schema()`](https://usrbinr.github.io/md/reference/create_schema.md),
[`create_table()`](https://usrbinr.github.io/md/reference/create_table.md),
[`delete_and_create_schema()`](https://usrbinr.github.io/md/reference/delete_and_create_schema.md),
[`delete_database()`](https://usrbinr.github.io/md/reference/delete_database.md),
[`delete_table()`](https://usrbinr.github.io/md/reference/delete_table.md),
[`describe_share()`](https://usrbinr.github.io/md/reference/describe_share.md),
[`drop_share()`](https://usrbinr.github.io/md/reference/drop_share.md),
[`list_owned_shares()`](https://usrbinr.github.io/md/reference/list_owned_shares.md),
[`list_shared_with_me_shares()`](https://usrbinr.github.io/md/reference/list_shared_with_me_shares.md),
[`upload_database_to_md()`](https://usrbinr.github.io/md/reference/upload_database_to_md.md)
