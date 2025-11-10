# Create or Append a Table from a Tibble or DBI-Backed Table

A thin wrapper that routes to either
[`create_table_dbi()`](https://usrbinr.github.io/md/reference/create_table_dbi.md)
(for `dbplyr`-backed lazy tables, class `"tbl_dbi"`) or
[`create_table_tbl()`](https://usrbinr.github.io/md/reference/create_table_tbl.md)
(for in-memory tibbles / data frames), creating a physical table in the
target database/schema. Supports **overwrite** and **append** write
strategies and defers all heavy lifting to the specific implementation.

## Usage

``` r
create_table(
  .data,
  .con,
  database_name,
  schema_name,
  table_name,
  write_type = "overwrite"
)
```

## Arguments

- .data:

  Tibble/data frame (in-memory) or a `dbplyr`/DBI-backed lazy table
  (class `"tbl_dbi"`).

- .con:

  A valid `DBI` connection (DuckDB / MotherDuck).

- database_name:

  Database name to create/use.

- schema_name:

  Schema name to create/use.

- table_name:

  Target table name to create or append to.

- write_type:

  Write strategy: `"overwrite"` (drop/create) or `"append"` (insert
  rows). Defaults to `"overwrite"`.

## Value

Invisibly returns `NULL`. Side effect: writes a table to the database by
delegating to the appropriate helper.

## Details

- If `.data` is a `dbplyr` lazy table (class `"tbl_dbi"`), the call is
  delegated to
  [`create_table_dbi()`](https://usrbinr.github.io/md/reference/create_table_dbi.md).

- If `.data` is an in-memory tibble/data frame (class including
  `"data.frame"`), the call is delegated to
  [`create_table_tbl()`](https://usrbinr.github.io/md/reference/create_table_tbl.md).

- Any other input classes trigger an error.

## See also

Other db-manage:
[`alter_table_schema()`](https://usrbinr.github.io/md/reference/alter_table_schema.md),
[`copy_tables_to_new_location()`](https://usrbinr.github.io/md/reference/copy_tables_to_new_location.md),
[`create_database()`](https://usrbinr.github.io/md/reference/create_database.md),
[`create_if_not_exists_share()`](https://usrbinr.github.io/md/reference/create_if_not_exists_share.md),
[`create_or_replace_share()`](https://usrbinr.github.io/md/reference/create_or_replace_share.md),
[`create_schema()`](https://usrbinr.github.io/md/reference/create_schema.md),
[`delete_and_create_schema()`](https://usrbinr.github.io/md/reference/delete_and_create_schema.md),
[`delete_database()`](https://usrbinr.github.io/md/reference/delete_database.md),
[`delete_schema()`](https://usrbinr.github.io/md/reference/delete_schema.md),
[`delete_table()`](https://usrbinr.github.io/md/reference/delete_table.md),
[`describe_share()`](https://usrbinr.github.io/md/reference/describe_share.md),
[`drop_share()`](https://usrbinr.github.io/md/reference/drop_share.md),
[`list_owned_shares()`](https://usrbinr.github.io/md/reference/list_owned_shares.md),
[`list_shared_with_me_shares()`](https://usrbinr.github.io/md/reference/list_shared_with_me_shares.md),
[`upload_database_to_md()`](https://usrbinr.github.io/md/reference/upload_database_to_md.md)
