# Create or Append a Table from a Tibble or DBI-Backed Table

A thin wrapper that routes to either
[`create_table_dbi()`](create_table_dbi.md) (for `dbplyr`-backed lazy
tables, class `"tbl_dbi"`) or
[`create_table_tbl()`](create_table_tbl.md) (for in-memory tibbles /
data frames), creating a physical table in the target database/schema.
Supports **overwrite** and **append** write strategies and defers all
heavy lifting to the specific implementation.

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
  delegated to [`create_table_dbi()`](create_table_dbi.md).

- If `.data` is an in-memory tibble/data frame (class including
  `"data.frame"`), the call is delegated to
  [`create_table_tbl()`](create_table_tbl.md).

- Any other input classes trigger an error.

## See also

Other db-manage: [`alter_table_schema()`](alter_table_schema.md),
[`copy_tables_to_new_location()`](copy_tables_to_new_location.md),
[`create_database()`](create_database.md),
[`create_if_not_exists_share()`](create_if_not_exists_share.md),
[`create_or_replace_share()`](create_or_replace_share.md),
[`create_schema()`](create_schema.md),
[`delete_and_create_schema()`](delete_and_create_schema.md),
[`delete_database()`](delete_database.md),
[`delete_schema()`](delete_schema.md),
[`delete_table()`](delete_table.md),
[`describe_share()`](describe_share.md),
[`drop_share()`](drop_share.md),
[`list_owned_shares()`](list_owned_shares.md),
[`list_shared_with_me_shares()`](list_shared_with_me_shares.md),
[`upload_database_to_md()`](upload_database_to_md.md)
